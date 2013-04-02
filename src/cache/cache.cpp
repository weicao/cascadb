// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <sstream>

#include "util/logger.h"
#include "cache.h"

using namespace std;
using namespace cascadb;

static void* flusher_main(void *arg)
{
    Cache *cache = (Cache*) arg;
    cache->write_back();
    return NULL;
}

class LRUComparator {
public:
    bool operator() (Node* x, Node* y)
    {
        return x->get_last_used_timestamp() < y->get_last_used_timestamp();
    }
};

class FirstWriteComparator {
public:
    bool operator() (Node* x, Node* y)
    {
        return x->get_first_write_timestamp() < y->get_first_write_timestamp();
    }
};

Cache::Cache(const Options& options)
: options_(options), 
  size_(0),
  alive_(false),
  flusher_(NULL)
{
}

Cache::~Cache()
{
    alive_ = false;
    if (flusher_) {
        flusher_->join();
        delete flusher_;
    }
}

bool Cache::init()
{
    assert(!alive_);

    alive_ = true;
    flusher_ = new Thread(flusher_main);
    if (flusher_) {
        flusher_->start(this);
        return true;
    }
    
    LOG_ERROR("cannot create flusher thread");
    return false;
}

bool Cache::add_table(const std::string& tbn, NodeFactory *factory, Layout *layout)
{
    tables_lock_.write_lock();
    if (tables_.find(tbn) != tables_.end()) {
        tables_lock_.unlock();
        LOG_ERROR("table " << tbn << " already registered in cache");
        return false;
    }

    TableSettings tbs;
    tbs.factory = factory;
    tbs.layout = layout;

    tables_[tbn] = tbs;
    tables_lock_.unlock();
    return true;
}

void Cache::flush_table(const std::string& tbn)
{
    TableSettings tbs;
    if(!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    Layout *layout = tbs.layout;
    vector<Node*> zombies;
    vector<Node*> dirty_nodes;
    size_t dirty_size = 0;

    ScopedMutex global_lock(&global_mtx_);

    nodes_lock_.write_lock();
    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        if (it->first.tbn  == tbn) {
            Node *node = it->second;
            if (node->is_dead()) {
                zombies.push_back(node);
                nodes_.erase(it);
            } else {
                size_t sz = node->size();
                // TODO: flush all node
                if (node->is_dirty() && !node->is_flushing() && node->pin() == 0) {
                    node->write_lock();
                    node->set_flushing(true);
                    dirty_nodes.push_back(node);
                    dirty_size += sz;
                }
            }
        }
    }
    nodes_lock_.unlock();

    if (dirty_nodes.size()) {
        LOG_INFO("flush table " << tbn << ", write " << dirty_nodes.size() << " nodes, "
            << dirty_size << " bytes total");
        flush_nodes(dirty_nodes);
    }

    if (zombies.size()) {
        LOG_INFO("flush table " << tbn << ", delete " << zombies.size() << " nodes");
        delete_nodes(zombies);
    }

    global_lock.unlock();

    layout->flush();
}

void Cache::del_table(const std::string& tbn, bool flush)
{
    if(flush) {
        flush_table(tbn);
    }

    tables_lock_.write_lock();
    map<string, TableSettings>::iterator it = tables_.find(tbn);
    if (it == tables_.end()) {
        tables_lock_.unlock();
        return;
    }
    tables_.erase(it);
    tables_lock_.unlock();

    size_t total_count = 0;

    ScopedMutex global_lock(&global_mtx_);

    nodes_lock_.write_lock();
    // TODO: improve me
    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        if (it->first.tbn  == tbn) {
            Node *node = it->second;
            assert(node->ref() == 0);
            delete node;
            
            nodes_.erase(it);
            total_count ++;
        }
    }
    nodes_lock_.unlock();

    global_lock.unlock();

    LOG_INFO("release " << total_count << " nodes in table " << tbn);
}

void Cache::put(const std::string& tbn, bid_t nid, Node* node)
{
    assert(node->ref() == 0);

    CacheKey key(tbn, nid);
    TableSettings tbs;
    if (!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    while (must_evict()) {
        evict();
    }

    nodes_lock_.write_lock();
    assert(nodes_.find(key) == nodes_.end());
    nodes_[key] = node;
    node->inc_ref();
    nodes_lock_.unlock();
}

Node* Cache::get(const std::string& tbn, bid_t nid, bool skeleton_only)
{
    CacheKey key(tbn, nid);
    Node *node;

    TableSettings tbs;
    if (!get_table_settings(tbn, tbs)) {
        assert(false);
    }

    nodes_lock_.read_lock();
    map<CacheKey, Node*>::iterator it = nodes_.find(key);
    if (it != nodes_.end()) {
        node = it->second;
        node->inc_ref();
        nodes_lock_.unlock();
        return node;
    }
    nodes_lock_.unlock();

    while (must_evict()) {
        evict();
    }
    
    Block* block = tbs.layout->read(nid, skeleton_only);
    if (block == NULL) return NULL;
    
    node = tbs.factory->new_node(nid);
    BlockReader reader(block);
    if (!node->read_from(reader, skeleton_only)) {
        assert(false);
    }
    tbs.layout->destroy(block);
    
    nodes_lock_.write_lock();
    it = nodes_.find(key);
    if (it != nodes_.end()) {
        LOG_WARN("detect multiple threads're loading node " << nid << " concurrently");
        delete node;
        node = it->second;
    } else {
        nodes_[key] = node;
    }
    node->inc_ref();
    nodes_lock_.unlock();

    return node;
}

bool Cache::get_table_settings(const std::string& tbn, TableSettings& tbs)
{
    tables_lock_.read_lock();
    map<string, TableSettings>::iterator it = tables_.find(tbn);
    if (it != tables_.end()) {
        tbs = it->second;
        tables_lock_.unlock();
        return true;
    }
    tables_lock_.unlock();
    return false;
}

bool Cache::must_evict()
{
    ScopedMutex lock(&size_mtx_);

    return size_ >= options_.cache_limit;
}

bool Cache::need_evict()
{
    ScopedMutex lock(&size_mtx_);

    size_t threshold = (options_.cache_limit * 
        options_.cache_evict_high_watermark) / 100;

    return size_ > threshold;
}

void Cache::evict()
{
    size_t total_size = 0;
    size_t total_count = 0;

    size_t active_size = 0;
    size_t active_count = 0;

    size_t dirty_size = 0;
    size_t dirty_count = 0;

    size_t flushing_size = 0;
    size_t flushing_count = 0;

    size_t clean_size = 0;
    size_t clean_count = 0;

    vector<Node*> zombies;
    vector<Node*> clean_nodes;

    nodes_lock_.write_lock();

    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {

        Node *node = it->second;
        assert(node->nid() == it->first.nid);

        if (node->is_dead()) {
            if (node->ref() == 0) {
                zombies.push_back(node);
                nodes_.erase(it);
            }
        } else {
            size_t size = node->size();

            total_size += size;
            total_count ++;

            if (node->ref()) {
                active_size += size;
                active_count ++;
            }

            if (node->is_dirty()) {
                dirty_size += size;
                dirty_count ++;
            }

            if (node->is_flushing()) {
                flushing_size += size;
                flushing_count ++;
            }

            // check everything again after ensure reference is 0
            if (node->ref() == 0 && !node->is_dead() 
                    && !node->is_dirty() && !node->is_flushing()) {
                clean_size += size;
                clean_count ++;
                clean_nodes.push_back(node);
            }
        }
    }

    ScopedMutex size_lock(&size_mtx_);
    // update size
    size_ = total_size;
    size_lock.unlock();
    
    LRUComparator comp;
    sort(clean_nodes.begin(), clean_nodes.end(), comp);

    size_t goal = (options_.cache_limit * options_.cache_evict_ratio)/100;
    size_t evicted_size = 0;
    size_t evicted_count = 0;
    
    for(size_t i = 0; i < clean_nodes.size(); i++) {
        if (evicted_size >= goal) break;

        Node *node = clean_nodes[i];

        // reference counting should keep zero when node map is locked,
        // so nobody outside the cache can modify this node,
        // so it is impossible to be flushed out
        assert(node->ref() == 0 && !node->is_dirty() && !node->is_flushing());

        // one and only one node is erased
        if (nodes_.erase(CacheKey(node->table_name(), node->nid())) != 1) {
            assert(false);
        }

        evicted_size += node->size();
        evicted_count ++;
        delete node;
    }

    size_lock.lock();
    // update size
    assert(size_ >= evicted_size);
    size_ -= evicted_size;
    size_lock.unlock();

    nodes_lock_.unlock();

    // clear zombies
    if (zombies.size()) {
        delete_nodes(zombies);
    }

#ifdef DEBUG_CACHE
    LOG_TRACE("Total " << total_count << " nodes (" 
        << total_size << " bytes), " 
        << active_count << " active nodes ("
        << active_size << " bytes), "
        << dirty_count << " dirty nodes ("
        << dirty_size << " bytes), "
        << flushing_count << " flushing nodes ("
        << flushing_size << " bytes), "
        << clean_count << " clean nodes ("
        << clean_size << " bytes), "
        << "Evict " << evicted_count << " nodes ("
        << evicted_size << " bytes), "
        << "Delete " << zombies.size() << " zombie nodes");
#endif
}

void Cache::write_back()
{
    while(alive_) {
        Time current = now();
        FirstWriteComparator comparator;
        size_t goal = (options_.cache_limit * 
            options_.cache_writeback_ratio)/100;

        size_t total_size = 0;
        size_t total_count = 0;

        size_t active_size = 0;
        size_t active_count = 0;

        size_t dirty_size = 0;
        size_t dirty_count = 0;

        vector<Node*> expired_nodes;
        size_t expired_size = 0;

        nodes_lock_.read_lock();
        for(map<CacheKey, Node*>::iterator it = nodes_.begin();
            it != nodes_.end(); it++ ) {
            Node *node = it->second;

            if (!node->is_dead()) {
                size_t sz = node->size();

                total_size += sz;
                total_count ++;
                if (node->ref()) {
                    active_size += sz;
                    active_count ++;
                }

                if (node->is_dirty()) {
                    dirty_size += sz;
                    dirty_count ++;

                    bool expired = interval_us(node->get_first_write_timestamp(),
                        current) > options_.cache_dirty_expire * 1000;

                    // do not write node until last write is completed
                    if ( expired && !node->is_flushing() && node->pin() == 0) {
                        expired_nodes.push_back(node);
                        expired_size += sz;
                    }
                }
            }
        }

        ScopedMutex size_lock(&size_mtx_);
        // update size
        size_ = total_size;
        size_lock.unlock();

        nodes_lock_.unlock();

        vector<Node*> flushed_nodes;
        size_t flushed_size = 0;

        sort(expired_nodes.begin(), expired_nodes.end(), comparator);

        for (size_t i = 0; i < expired_nodes.size(); i++ ) {
            if (flushed_size >= goal) break;

            Node *node = expired_nodes[i];

            // set write lock on node
            if (node->try_write_lock()) {
                // check again
                if (node->pin() == 0 && !node->is_dead()) {
                    node->set_flushing(true);
                    flushed_nodes.push_back(node);
                    flushed_size += node->size();
                } else {
                    node->unlock();
                }
            }
        }

        // need to flush more dirty pages
        if ((dirty_size - flushed_size) >= (options_.cache_limit * 
            options_.cache_dirty_high_watermark)/100 && flushed_size < goal) {

            vector<Node*> candidates;

            nodes_lock_.read_lock();
            for (map<CacheKey, Node*>::iterator it = nodes_.begin();
                it != nodes_.end(); it++ ) {
                Node *node = it->second;

                if (node->is_dirty() && node->pin() == 0 
                    && !node->is_flushing() && !node->is_dead()) {
                    candidates.push_back(node);
                }
            }
            nodes_lock_.unlock();

            sort(candidates.begin(), candidates.end(), comparator);

            for (size_t i = 0; i < candidates.size(); i++ ) {
                if (flushed_size >= goal) break;

                Node *node = candidates[i];

                // set write lock on node
                if (node->try_write_lock()) {
                    // check again
                    if (node->pin() == 0 && !node->is_dead()) {
                        node->set_flushing(true);
                        flushed_nodes.push_back(node);
                        flushed_size += node->size();
                    } else {
                        node->unlock();
                    }
                }
            }
        }
        
        // flush
        if (flushed_nodes.size()) {
            flush_nodes(flushed_nodes);
        }

#ifdef DEBUG_CACHE        
        LOG_TRACE("Total " << total_count << " nodes (" 
            << total_size << " bytes), " 
            << active_count << " active nodes ("
            << active_size << " bytes), "
            << dirty_count << " dirty nodes ("
            << dirty_size << " bytes), "
            << "Flush " << flushed_nodes.size() << " nodes ("
            << flushed_size << " bytes)");
#endif
        if (need_evict()) {
            evict();
        } else {
            usleep(options_.cache_writeback_interval * 1000);
        }
    }
}

void Cache::flush_nodes(vector<Node*>& nodes)
{
    LOG_TRACE("flush " << nodes.size() << " nodes");

    for (size_t i = 0 ; i < nodes.size(); i++) {
        Node* node = nodes[i];
        bid_t nid = node->nid();

        // TODO: test node is write locked

        TableSettings tbs;
        if (!get_table_settings(node->table_name(), tbs)) {
            assert(false);
        }
        Layout *layout = tbs.layout;
        assert(layout);

        size_t buffer_size = node->estimated_buffer_size();
       
        Block *block = layout->create(buffer_size);
        assert(block);
        
        size_t skeleton_size;
        BlockWriter writer(block);
        if (!node->write_to(writer, skeleton_size)) {
            assert(false);
        }
        assert(buffer_size >= block->size());
        node->set_dirty(false);

        // unlock node
        node->unlock();
        
        WriteCompleteContext *context = new WriteCompleteContext();
        context->node = node;
        context->layout = layout;
        context->block =block;
        Callback *cb = new Callback(this, &Cache::write_complete, context);
        layout->async_write(nid, block, skeleton_size, cb);
    }
}

void Cache::write_complete(WriteCompleteContext* context, bool succ)
{
    Node *node = context->node;
    assert(node);
    Layout *layout = context->layout;
    assert(layout);
    Block *block = context->block;
    assert(block);

    if (succ) {
        LOG_TRACE("write node table " << node->table_name() << ", nid " << node->nid() << " ok" );
    } else {
        LOG_ERROR("write node table " << node->table_name() << ", nid " << node->nid() << " error");
        // TODO: handle the error
    }

    node->set_flushing(false);
    layout->destroy(block);
    delete context;
}

void Cache::delete_nodes(vector<Node*>& nodes)
{
    LOG_TRACE("delete " << nodes.size() << " nodes");

    for (size_t i = 0; i < nodes.size(); i++) {
        Node* node = nodes[i];
        bid_t nid = node->nid();

        // TODO: check node is stored to layout
        TableSettings tbs;
        if (!get_table_settings(node->table_name(), tbs)) {
            assert(false);
        }

        delete node;
        
        Layout *layout = tbs.layout;
        assert(layout);
       
        layout->delete_block(nid);
    }
}

void Cache::debug_print(std::ostream& out)
{
    size_t total_size = 0;
    size_t total_count = 0;

    size_t active_size = 0;
    size_t active_count = 0;

    size_t dirty_size = 0;
    size_t dirty_count = 0;

    size_t flushing_size = 0;
    size_t flushing_count = 0;

    size_t clean_size = 0;
    size_t clean_count = 0;

    nodes_lock_.read_lock();

    for(map<CacheKey, Node*>::iterator it = nodes_.begin();
        it != nodes_.end(); it++ ) {
        Node *node = it->second;
        if (!node->is_dead()) {
            size_t size = node->size();

            total_size += size;
            total_count ++;
            if (node->ref()) {
                active_size += size;
                active_count ++;
            }

            if (node->is_dirty()) {
                dirty_size += size;
                dirty_count ++;
            } else if (node->is_flushing()) {
                flushing_size += size;
                flushing_count ++;
            } else {
                clean_size += size;
                clean_count ++;
            }
        }
    }

    nodes_lock_.unlock();

    out << "### Dump Cache ###" << endl;
    out << "Total " << total_count << " nodes (" 
        << total_size << " bytes), " 
        << active_count << " active nodes ("
        << active_size << " bytes), "
        << dirty_count << " dirty nodes ("
        << dirty_size << " bytes), "
        << flushing_count << " flushing nodes ("
        << flushing_size << " bytes), "
        << clean_count << " clean nodes ("
        << clean_size << " bytes), "
        << endl;
}
