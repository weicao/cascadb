// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <algorithm>
#include <set>
#include <stdexcept>

#include "node.h"
#include "tree.h"
#include "keycomp.h"
#include "util/logger.h"
#include "util/crc16.h"
#include "util/bloom.h"

using namespace std;
using namespace cascadb;

/********************************************************
                        SchemaNode 
*********************************************************/

#define SCHEMA_NODE_SIZE 32

size_t SchemaNode::size()
{
    return SCHEMA_NODE_SIZE;
}

size_t SchemaNode::estimated_buffer_size()
{
    return SCHEMA_NODE_SIZE;
}

bool SchemaNode::read_from(BlockReader& reader, bool skeleton_only)
{
    if (!reader.readUInt64(&root_node_id)) return false;
    if (!reader.readUInt64(&next_inner_node_id)) return false;
    if (!reader.readUInt64(&next_leaf_node_id)) return false;
    if (!reader.readUInt64(&tree_depth)) return false;

    return true;
}

bool SchemaNode::write_to(BlockWriter& writer, size_t& skeleton_size)
{
    if (!writer.writeUInt64(root_node_id)) return false;
    if (!writer.writeUInt64(next_inner_node_id)) return false;
    if (!writer.writeUInt64(next_leaf_node_id)) return false;
    if (!writer.writeUInt64(tree_depth)) return false;
    skeleton_size = SCHEMA_NODE_SIZE;
    return true;
}

/********************************************************
                        InnerNode
*********************************************************/

InnerNode::~InnerNode()
{
    delete first_msgbuf_;
    first_msgbuf_ = NULL;
    for(vector<Pivot>::iterator it = pivots_.begin();
        it != pivots_.end(); it++) {
        it->key.destroy();
        delete it->msgbuf;
    }
    pivots_.clear();
}

void InnerNode::init_empty_root()
{
    assert(first_msgbuf_ == NULL);
    // create the first child for root
    first_msgbuf_ = new MsgBuf(tree_->options_.comparator);
    msgbufsz_ = first_msgbuf_->size();
    bottom_ = true;
    set_dirty(true);
}

bool InnerNode::write(const Msg& m)
{
    read_lock();

    // If the tree has been grown up, we must 
    // rewrite this msg from the newly created root.
    if (tree_->root()->nid() != nid_) {
        unlock(); 
        return tree_->root()->write(m);
    }

    if (status_ == kSkeletonLoaded) {
        load_all_msgbuf();
    }

    Slice k = m.key;
    insert_msgbuf(m, find_pivot(k));
    set_dirty(true);
    
    maybe_cascade();
    return true;
}

bool InnerNode::cascade(MsgBuf *mb, InnerNode* parent){
    read_lock();

    // lazy load
    if (status_ == kSkeletonLoaded) {
        load_all_msgbuf();
    }

    // lock message buffer
    mb->write_lock();
    size_t oldcnt = mb->count();
    size_t oldsz = mb->size();

    MsgBuf::Iterator rs, it, end;
    rs = it = mb->begin(); // range start
    end = mb->end(); // range end
    size_t i = 0;
    while (it != end && i < pivots_.size()) {
        if( comp_pivot(it->key, i) < 0 ) {
            it ++;
        } else {
            if (rs != it) {
                insert_msgbuf(rs, it, i);
                rs = it;
            }
            i ++;
        }
    }
    if(rs != end) {
        insert_msgbuf(rs, end, i);
    }

    // clear message buffer and modify parent's status
    mb->clear();
    parent->msgcnt_ = parent->msgcnt_ + mb->count() - oldcnt;
    parent->msgbufsz_ = parent->msgbufsz_ + mb->size() - oldsz;

    // unlock message buffer
    mb->unlock();
    // crab walk
    parent->unlock();

    set_dirty(true);
    maybe_cascade();
    
    return true;
}

int InnerNode::comp_pivot(Slice k, int i)
{
    assert(i >=0 && (size_t)i < pivots_.size());
    return tree_->options_.comparator->compare(k, pivots_[i].key);
}

int InnerNode::find_pivot(Slice k)
{
    // int idx = 0;
    // for(vector<Pivot>::iterator it = pivots_.begin();
    //     it != pivots_.end(); it++ ) {
    //     if (tree_->options_.comparator->compare(k, it->key) < 0) {
    //         return idx;
    //     }
    //     idx ++;
    // }
    // return idx;

    // optimize for sequential write
    size_t size = pivots_.size();
    if (size && tree_->options_.comparator->compare(pivots_[size-1].key, k) < 0) {
        return size;
    }

    vector<Pivot>::iterator first = pivots_.begin();
    vector<Pivot>::iterator last = pivots_.end();
    vector<Pivot>::iterator it;

    // binary search
    iterator_traits<vector<Pivot>::iterator>::difference_type count, step;
    count = distance(first,last);
    while (count > 0) {
        it = first; 
        step = count/2; 
        advance(it, step);

        if (tree_->options_.comparator->compare(it->key, k) <= 0) {
            first = ++ it;
            count -= step + 1;
        } else {
            count = step;
        }
    }

    return distance(pivots_.begin(), first);
}

MsgBuf* InnerNode::msgbuf(int idx)
{
    assert(idx >= 0 && (size_t)idx <= pivots_.size());

    MsgBuf **pb = (idx == 0) ? &first_msgbuf_ : &(pivots_[idx-1].msgbuf);
    if (*pb) {
        return *pb;
    } else {
        assert(status_ == kSkeletonLoaded);
        load_msgbuf(idx);
        return *pb;
    }
}

MsgBuf* InnerNode::msgbuf(int idx, Slice& key)
{
    assert(idx >= 0 && (size_t)idx <= pivots_.size());

    Slice *filter = (idx == 0) ? &first_filter_ : &pivots_[idx-1].filter;
    MsgBuf **pb = (idx == 0) ? &first_msgbuf_ : &(pivots_[idx-1].msgbuf);
    if (*pb) {
        return *pb;
    } else {
        assert(status_ == kSkeletonLoaded);
        // i am not in this msgbuf, don't to load msgbuf
       if (bloom_matches(key, *filter))
            load_msgbuf(idx);

        return *pb;
    }
}

bid_t InnerNode::child(int idx)
{
    assert(idx >= 0 && (size_t)idx <= pivots_.size());
    if (idx == 0) {
        return first_child_;
    } else {
        return pivots_[idx-1].child;
    }
}

void InnerNode::set_child(int idx, bid_t c)
{
    assert(idx >= 0 && (size_t)idx <= pivots_.size());
    if (idx == 0) {
        first_child_ = c;
    } else {
        pivots_[idx-1].child = c;
    }
}

void InnerNode::insert_msgbuf(const Msg& m, int idx)
{
    MsgBuf *b = msgbuf(idx);
    assert(b);
    
    b->write_lock();
    size_t oldcnt = b->count();
    size_t oldsz = b->size();

    b->write(m);

    msgcnt_ = msgcnt_ + b->count() - oldcnt;
    msgbufsz_ = msgbufsz_ + b->size() - oldsz;
    b->unlock();
}

void InnerNode::insert_msgbuf(MsgBuf::Iterator begin, 
                             MsgBuf::Iterator end, int idx)
{
    MsgBuf *b = msgbuf(idx);
    assert(b);

    b->write_lock();
    size_t oldcnt = b->count();
    size_t oldsz = b->size();

    b->append(begin, end);

    msgcnt_ = msgcnt_ + b->count() - oldcnt;
    msgbufsz_ = msgbufsz_ + b->size() - oldsz;
    b->unlock();
}

int InnerNode::find_msgbuf_maxcnt()
{
    int idx = 0, ret = 0;
    size_t maxcnt = first_msgbuf_->count();
    for(vector<Pivot>::iterator it = pivots_.begin();
        it != pivots_.end(); it++ ) {
        idx ++;
        if (it->msgbuf->count() > maxcnt ) {
            it->msgbuf->read_lock();
            maxcnt = it->msgbuf->count();
            it->msgbuf->unlock();
            ret = idx;
        }
    }
    return ret;
}

int InnerNode::find_msgbuf_maxsz()
{
    int idx = 0, ret = 0;
    size_t maxsz = first_msgbuf_->size();
    for(vector<Pivot>::iterator it = pivots_.begin();
        it != pivots_.end(); it++ ) {
        idx ++;
        if (it->msgbuf->size() > maxsz ) {
            it->msgbuf->read_lock();
            maxsz = it->msgbuf->size();
            it->msgbuf->unlock();
            ret = idx;
        }
    }
    return ret;
}

void InnerNode::maybe_cascade()
{
    int idx = -1;
    if (msgcnt_ >= tree_->options_.inner_node_msg_count) {
        idx = find_msgbuf_maxcnt();
    } else if (size() >= tree_->options_.inner_node_page_size) {
        idx = find_msgbuf_maxsz();
    } else {
        unlock();
        return;
    }
   
    assert(idx >= 0);
    MsgBuf* b = msgbuf(idx);
    bid_t nid = child(idx);

    DataNode *node = NULL;
    if (nid == NID_NIL) {
        // cannot be inner node
        assert(bottom_);
        node = tree_->new_leaf_node();
        set_child(idx, node->nid());
    } else {
        node = tree_->load_node(nid, false);
    }
    assert(node);
    node->cascade(b, this);
    node->dec_ref();

    // it's possible to cascade twice
    // lock is released in child, so it's nescessarty to obtain it again
    read_lock();
    if (msgcnt_ >= tree_->options_.inner_node_msg_count ||
        size() >= tree_->options_.inner_node_page_size) {
        maybe_cascade();
    } else {
        unlock();
    }
}

void InnerNode::add_pivot(Slice key, bid_t nid, std::vector<DataNode*>& path)
{
    assert(path.back() == this);

    if (status_ == kSkeletonLoaded) {
        load_all_msgbuf();
    }

    vector<Pivot>::iterator it = std::lower_bound(pivots_.begin(), 
        pivots_.end(), key, KeyComp(tree_->options_.comparator));
    MsgBuf* mb = new MsgBuf(tree_->options_.comparator);
    pivots_.insert(it, Pivot(key.clone(), nid, mb));
    pivots_sz_ += pivot_size(key);
    msgbufsz_ += mb->size();
    set_dirty(true);
    
    if (pivots_.size() + 1 > tree_->options_.inner_node_children_number) {
        split(path);
    } else {
        while(path.size()) {
            path.back()->unlock();
            path.back()->dec_ref();
            path.pop_back();
        }
    }
}

void InnerNode::split(std::vector<DataNode*>& path)
{
    assert(pivots_.size() > 1);
    size_t n = pivots_.size()/2;
    size_t n1 = pivots_.size() - n - 1;
    Slice k = pivots_[n].key;

    InnerNode *ni = tree_->new_inner_node();
    ni->bottom_ = IS_LEAF(pivots_[n].child);
    assert(ni);
    
    ni->first_child_ = pivots_[n].child;
    ni->first_msgbuf_ = pivots_[n].msgbuf;
    ni->pivots_.resize(n1);
    std::copy(pivots_.begin() + n + 1, pivots_.end(), ni->pivots_.begin());
    pivots_.resize(n);
    
    size_t pivots_sz1 = 0;
    size_t msgcnt1 = 0;
    size_t msgbufsz1 = 0;
    msgcnt1 += ni->first_msgbuf_->count();
    msgbufsz1 += ni->first_msgbuf_->size();
    for(size_t i = 0; i < ni->pivots_.size(); i++) {
        pivots_sz1 += pivot_size(ni->pivots_[i].key);
        msgcnt1 += ni->pivots_[i].msgbuf->count();
        msgbufsz1 += ni->pivots_[i].msgbuf->size();
    }
    ni->pivots_sz_ = pivots_sz1;
    ni->msgcnt_ = msgcnt1;
    ni->msgbufsz_ = msgbufsz1;
    pivots_sz_ -= (pivots_sz1 + pivot_size(k));
    msgcnt_ -= msgcnt1;
    msgbufsz_ -= msgbufsz1;
    
    ni->set_dirty(true);
    ni->dec_ref();

    path.pop_back();
    
    // propagation
    if( path.size() == 0) {
        // i'm root
        InnerNode *nr = tree_->new_inner_node();
        assert(nr);
        nr->bottom_ = false;
        nr->first_child_ = nid_;
        MsgBuf* mb0 = new MsgBuf(tree_->options_.comparator);
        nr->first_msgbuf_ = mb0;
        nr->msgbufsz_ += mb0->size();
        nr->pivots_.resize(1);
        MsgBuf* mb1 = new MsgBuf(tree_->options_.comparator);
        nr->pivots_[0] = Pivot(k.clone(), ni->nid_, mb1);
        nr->pivots_sz_ += pivot_size(k);
        nr->msgbufsz_ += mb1->size();
        nr->set_dirty(true);
        
        tree_->pileup(nr);
        
        // need not do nr->dec_ref() here
    } else {
        // propagation
        InnerNode* parent = (InnerNode*) path.back();
        assert(parent);
        parent->add_pivot(k, ni->nid_, path);
    }

    // Almost no loss of efficiency if we place unlock() 
    // in the end of this function.
    
    // For some extremely lock contention environment,
    // we must hold the write_lock() untill the tree pileup.
    unlock();
    dec_ref();
}

void InnerNode::rm_pivot(bid_t nid, std::vector<DataNode*>& path)
{
    // todo free memory of pivot key

    assert(path.back() == this);

    if (status_ == kSkeletonLoaded) {
        load_all_msgbuf();
    }

    if (first_child_ == nid) {
        /// @todo this is true only for single thread, fix me
        assert(first_msgbuf_->count() == 0);
        msgbufsz_ -= first_msgbuf_->size();
        delete first_msgbuf_;
        
        if (pivots_.size() == 0) {
            first_msgbuf_ = NULL;
            dead_ = true;

            path.pop_back();
            unlock();
            dec_ref();

            if (path.size() == 0) {
                // reach root
                tree_->collapse(); 
            } else {
                // propagation
                InnerNode* parent = (InnerNode*) path.back();
                assert(parent);
                parent->rm_pivot(nid_, path);
            }
            return;
        }

        // shift pivots
        first_child_ = pivots_[0].child;
        first_msgbuf_ = pivots_[0].msgbuf;

        pivots_sz_ -= pivot_size(pivots_[0].key);
        pivots_.erase(pivots_.begin());

        // TODO adjst size
    } else {
        vector<Pivot>::iterator it;
        for (it = pivots_.begin(); 
            it != pivots_.end(); it ++) {
            if (it->child == nid) {
                break;
            }
        }

        assert(it != pivots_.end());
        /// @todo this is true only for single thread, fix me
        assert(it->msgbuf->count() == 0);
        msgbufsz_ -= it->msgbuf->size();
        delete it->msgbuf;

        pivots_sz_ -= pivot_size(it->key);
        pivots_.erase(it);

        // TODO adjust size
    }

    set_dirty(true);

    // unlock all parents
    while (path.size()) {
        path.back()->unlock();
        path.back()->dec_ref();
        path.pop_back();
    }
}

bool InnerNode::find(Slice key, Slice& value, InnerNode *parent)
{
    bool ret = false;
    read_lock();

    if (parent) {
        parent->unlock(); // lock coupling
    }

    int idx = find_pivot(key);

    MsgBuf* b = msgbuf(idx, key);
    // if b is NULL, means rejected by bloom filter
    if (b) {
        b->read_lock(); 
        MsgBuf::Iterator it = b->find(key);
        if (it != b->end() && it->key == key ) {
            if (it->type == Put) {
                value = it->value.clone();
                ret = true;
            }
            // otherwise deleted
            b->unlock();
            unlock();
            return ret;
        }
        b->unlock();
    }

    bid_t chidx = child(idx);
    if (chidx == NID_NIL) {
        assert(idx == 0); // must be the first child
        unlock();
        return false;
    }
    
    // find in child
    DataNode* ch = tree_->load_node(chidx, true);
    assert(ch);
    ret = ch->find(key, value, this);
    ch->dec_ref();
    return ret;
}

void InnerNode::lock_path(Slice key, std::vector<DataNode*>& path)
{
    int idx = find_pivot(key);
    DataNode* ch = tree_->load_node(child(idx), false);
    assert(ch);
    ch->write_lock();
    path.push_back(ch);
    ch->lock_path(key, path);
}

size_t InnerNode::pivot_size(Slice key)
{
    return 4 + key.size() + // key
                        8 + // child nid size
                        4 + // msgbuf offset
                        4 + // msgbuf length
                        4 + // msgbuf uncompressed length
                        2;// msgbuf crc
}

size_t InnerNode::bloom_size(int n)
{
    return 4 + cascadb::bloom_size(n); //bloom sizes
}


size_t InnerNode::size()
{
    size_t sz = 0;
    sz += 1 + 4 + (8 + 4 + 4 + 4 + 2);
    sz += pivots_sz_;
    sz += msgbufsz_;
    return sz;
}

size_t InnerNode::estimated_buffer_size()
{
    size_t sz = 0;
    sz += 1 + 4 + (8 + 4 + 4 + 4 + 2);
    // first msgbuf bloom bitsets
    sz += bloom_size(first_msgbuf_->count());
    sz += pivots_sz_;

    if (tree_->compressor_) {
        sz += tree_->compressor_->max_compressed_length(first_msgbuf_->size());
        for (size_t i = 0; i < pivots_.size(); i++) {
            sz += tree_->compressor_->max_compressed_length(pivots_[i].msgbuf->size());
            sz += bloom_size(pivots_[i].msgbuf->count());
        }
    } else {
        sz += first_msgbuf_->size();
        for (size_t i = 0; i < pivots_.size(); i++) {
            sz += pivots_[i].msgbuf->size();
            sz += bloom_size(pivots_[i].msgbuf->count());
        }
    }
    return sz;
}

bool InnerNode::read_from(BlockReader& reader, bool skeleton_only)
{
    if (!reader.readBool(&bottom_)) return false;

    uint32_t pn;
    if (!reader.readUInt32(&pn)) return false;

    pivots_.resize(pn);

    if (!reader.readUInt64(&first_child_)) return false;
    if (!reader.readUInt32(&first_msgbuf_offset_)) return false;
    if (!reader.readUInt32(&first_msgbuf_length_)) return false;
    if (!reader.readUInt32(&first_msgbuf_uncompressed_length_)) return false;
    if (!reader.readUInt16(&first_msgbuf_crc_)) return false;
    if (!reader.readSlice(first_filter_)) return false;

    for (size_t i = 0; i < pn; i++) {
        if (!reader.readSlice(pivots_[i].key)) return false;
        pivots_sz_ += pivot_size(pivots_[i].key);

        if (!reader.readUInt64(&(pivots_[i].child))) return false;
        pivots_[i].msgbuf = NULL; // unloaded
        if (!reader.readUInt32(&(pivots_[i].offset))) return false;
        if (!reader.readUInt32(&(pivots_[i].length))) return false;
        if (!reader.readUInt32(&(pivots_[i].uncompressed_length))) return false;
        if (!reader.readUInt16(&(pivots_[i].crc))) return false;
        if (!reader.readSlice(pivots_[i].filter)) return false;
    }

    if (!skeleton_only) {
        if (!load_all_msgbuf(reader)) return false;
    } else {
        status_ = kSkeletonLoaded;
    }

    return true;
}

bool InnerNode::load_msgbuf(int idx)
{
    uint32_t offset;
    uint32_t length;
    uint32_t uncompressed_length;
    uint16_t expected_crc;
    uint16_t actual_crc;
    if (idx == 0) {
        offset = first_msgbuf_offset_;
        length = first_msgbuf_length_;
        uncompressed_length = first_msgbuf_uncompressed_length_;
        expected_crc = first_msgbuf_crc_;
    } else {
        offset = pivots_[idx-1].offset;
        length = pivots_[idx-1].length;
        uncompressed_length = pivots_[idx-1].uncompressed_length;
        expected_crc = pivots_[idx-1].crc;
    }

    Block* block = tree_->layout_->read(nid_, offset, length);
    if (block == NULL) {
        LOG_ERROR("read msgbuf from layout error " << " nid " << nid_ << ", idx " << idx
                << ", offset " << offset << ", length " << length);
        return false;
    }

    actual_crc = crc16(block->start(), length);
    if (actual_crc != expected_crc) {
        LOG_ERROR("msgbuf crc  error " << " nid " << nid_ << ", idx " << idx
                << ", expected_crc " << expected_crc
                << ", actual_crc " << actual_crc
                << ", offset " << offset 
                << ", length " << length);

        tree_->layout_->destroy(block);
        return false;
    }

    BlockReader reader(block);

    Slice buffer;
    if (tree_->compressor_) {
        buffer = Slice::alloc(uncompressed_length);
    }

    MsgBuf *b = new MsgBuf(tree_->options_.comparator);
    assert(b);

    if (!read_msgbuf(reader, length, uncompressed_length, b, buffer)) {
        LOG_ERROR("read_msgbuf error " << " nid " << nid_ << ", idx " << idx);
        delete b;
        if (buffer.size()) {
            buffer.destroy();
        }
        tree_->layout_->destroy(block);
        return false;
    }

    if (buffer.size()) {
        buffer.destroy();
    }

    // lazy load, upgrade lock to write lock
    // TODO: write a upgradable rwlock
    unlock();
    write_lock();

    MsgBuf **pb = (idx == 0) ? &first_msgbuf_ : &(pivots_[idx-1].msgbuf);
    if (*pb == NULL) {
        *pb = b;
        msgcnt_ += b->count();
        msgbufsz_ += b->size();
    } else {
        delete b;
    }

    unlock();
    read_lock();

    tree_->layout_->destroy(block);
    return true;
}

bool InnerNode::load_all_msgbuf()
{
    Block* block = tree_->layout_->read(nid_, false);
    if (block == NULL) {
        LOG_ERROR("load all msgbuf error, cannot read " << " nid " << nid_);
        return false;
    }

    BlockReader reader(block);

    // lazy load, upgrade lock to write lock
    // TODO: write a upgradable rwlock
    unlock();
    write_lock();

    bool ret = load_all_msgbuf(reader);

    unlock();
    read_lock();

    tree_->layout_->destroy(block);
    return ret;
}

bool InnerNode::load_all_msgbuf(BlockReader& reader)
{
    Slice buffer;
    if (tree_->compressor_) {
        size_t buffer_length = first_msgbuf_uncompressed_length_;
        for (size_t i = 0; i < pivots_.size(); i++) {
            if (buffer_length < pivots_[i].uncompressed_length) {
                buffer_length = pivots_[i].uncompressed_length;
            }
        }

        buffer = Slice::alloc(buffer_length);
    }

    if (first_msgbuf_ == NULL) {
        reader.seek(first_msgbuf_offset_);
        first_msgbuf_ = new MsgBuf(tree_->options_.comparator);
        if (!read_msgbuf(reader, first_msgbuf_length_,
                         first_msgbuf_uncompressed_length_, 
                         first_msgbuf_, buffer)) {
            if (buffer.size()) {
                buffer.destroy();
            }
            return false;
        }
        msgcnt_ += first_msgbuf_->count();
        msgbufsz_ += first_msgbuf_->size();
    }

    for (size_t i = 0; i < pivots_.size(); i++) {
        if (pivots_[i].msgbuf == NULL) {
            reader.seek(pivots_[i].offset);
            pivots_[i].msgbuf = new MsgBuf(tree_->options_.comparator);
            if (!read_msgbuf(reader, pivots_[i].length,
                             pivots_[i].uncompressed_length,
                             pivots_[i].msgbuf, buffer)) {
                if (buffer.size()) {
                    buffer.destroy();
                }
                return false;
            }
            msgcnt_ += pivots_[i].msgbuf->count();
            msgbufsz_ += pivots_[i].msgbuf->size();
        }
    }

    if (buffer.size()) {
        buffer.destroy();
    }

    status_ = kFullLoaded;
    return true;
}

bool InnerNode::read_msgbuf(BlockReader& reader,
                            size_t compressed_length, 
                            size_t uncompressed_length,
                            MsgBuf *mb, Slice buffer)
{
    if (tree_->compressor_) {
        assert(compressed_length <= reader.remain());
        assert(uncompressed_length <= buffer.size());

        // 1. uncompress
        if (!tree_->compressor_->uncompress(reader.addr(),
            compressed_length, (char *)buffer.data())) {
            return false;
        }
        reader.skip(compressed_length);

        // 2. deserialize
        Block block(buffer, 0, uncompressed_length);
        BlockReader rr(&block);
        return mb->read_from(rr);
    } else {
        return mb->read_from(reader);
    }
}

bool InnerNode::write_to(BlockWriter& writer, size_t& skeleton_size)
{
    // get length of skeleton and reserve space for skeleton
    size_t skeleton_offset = writer.pos();
    size_t skeleton_length = 1 + 4 + 8 + 4 + 4 + 4 + 2 +
        bloom_size(first_msgbuf_->count());

    for (size_t i = 0; i < pivots_.size(); i++) {
        skeleton_length += pivot_size(pivots_[i].key);
        skeleton_length += bloom_size(pivots_[i].msgbuf->count());
    }
    if (!writer.skip(skeleton_length)) return false;

    // prepare buffer if compression is enabled
    Slice buffer;
    if (tree_->compressor_) {
        // get buffer length to serialize msgbuf
        size_t buffer_length = first_msgbuf_->size();
        for (size_t i = 0; i < pivots_.size(); i++) {
            if (pivots_[i].msgbuf->size() > buffer_length)
                buffer_length = pivots_[i].msgbuf->size();
        }

        buffer = Slice::alloc(buffer_length);
    }

    char *mb_start;
    // write the first msgbuf
    mb_start = writer.addr();
    first_msgbuf_offset_ = writer.pos();
    if (!write_msgbuf(writer, first_msgbuf_, buffer)) return false;
    first_msgbuf_length_ = writer.pos() - first_msgbuf_offset_;
    first_msgbuf_uncompressed_length_ = first_msgbuf_->size();
    first_msgbuf_crc_ = crc16(mb_start, first_msgbuf_length_);

    // write rest msgbufs
    for (size_t i = 0; i < pivots_.size(); i++) {
        mb_start = writer.addr();
        pivots_[i].offset = writer.pos();
        if (!write_msgbuf(writer, pivots_[i].msgbuf, buffer)) return false;
        pivots_[i].length = writer.pos() - pivots_[i].offset;
        pivots_[i].uncompressed_length = pivots_[i].msgbuf->size();
        pivots_[i].crc = crc16(mb_start, pivots_[i].length);
    }

    if (buffer.size()) {
        buffer.destroy();
    }

    size_t last_offset = writer.pos();

    // seek to the head and write index
    writer.seek(skeleton_offset);


    if (!writer.writeBool(bottom_)) return false;
    if (!writer.writeUInt32(pivots_.size())) return false;

    if (!writer.writeUInt64(first_child_)) return false;
    if (!writer.writeUInt32(first_msgbuf_offset_)) return false;
    if (!writer.writeUInt32(first_msgbuf_length_)) return false;
    if (!writer.writeUInt32(first_msgbuf_uncompressed_length_)) return false;
    if (!writer.writeUInt16(first_msgbuf_crc_)) return false;

    // first msgbuf bloom filter
    std::string filter;
    first_msgbuf_->get_filter(&filter);
    first_filter_ = Slice(filter);
    if (!writer.writeSlice(first_filter_)) return false;
    filter.clear();

    for (size_t i = 0; i < pivots_.size(); i++) {
        if (!writer.writeSlice(pivots_[i].key)) return false;
        if (!writer.writeUInt64(pivots_[i].child)) return false;
        if (!writer.writeUInt32(pivots_[i].offset)) return false;
        if (!writer.writeUInt32(pivots_[i].length)) return false;
        if (!writer.writeUInt32(pivots_[i].uncompressed_length)) return false;
        if (!writer.writeUInt16(pivots_[i].crc)) return false;

	// get the bloom filter bitsets
        pivots_[i].msgbuf->get_filter(&filter);
        if (!writer.writeSlice(Slice(filter))) return false;
        filter.clear();
    }

    writer.seek(last_offset);
    skeleton_size = skeleton_length;
    return true;
}

bool InnerNode::write_msgbuf(BlockWriter& writer, MsgBuf *mb, Slice buffer)
{
    if (tree_->compressor_) {
        // 1. write to buffer
        Block block(buffer, 0, 0);
        BlockWriter wr(&block);
        if (!mb->write_to(wr)) return false;

        // 2. compress
        assert(tree_->compressor_->max_compressed_length(block.size()) <=
            writer.remain());

        size_t compressed_length;
        if (!tree_->compressor_->compress(buffer.data(), block.size(),
             writer.addr(), &compressed_length)) {
            LOG_ERROR("compress msgbuf error, nid " << nid_);
            return false;
        }

        // 3. skip
        writer.skip(compressed_length);

        return true;
    } else {
        return mb->write_to(writer);
    }
}

/********************************************************
                        LeafNode
*********************************************************/

LeafNode::LeafNode(const std::string& table_name, bid_t nid, Tree *tree)
: DataNode(table_name, nid, tree),
  balancing_(false),
  left_sibling_(NID_NIL),
  right_sibling_(NID_NIL),
  buckets_info_size_(0),
  records_(tree->options_.leaf_node_bucket_size)
{
    assert(nid >= NID_LEAF_START);
}

LeafNode::~LeafNode()
{
    for (size_t i = 0; i < buckets_info_.size(); i++ ) {
        buckets_info_[i].key.destroy();
    }

    for (size_t i = 0; i < records_.buckets_number(); i++ ) {
        RecordBucket *bucket = records_.bucket(i);
        if (bucket) {
            for (RecordBucket::iterator it = bucket->begin();
                it != bucket->end(); it++) {
                it->key.destroy();
                it->value.destroy();
            }
        }
    }
}

bool LeafNode::cascade(MsgBuf *mb, InnerNode* parent)
{
    write_lock();

    if (status_ == kSkeletonLoaded) {
        load_all_buckets();
    }

    // lock message buffer from parent
    mb->write_lock();
    size_t oldcnt = mb->count();
    size_t oldsz = mb->size();

    Slice anchor = mb->begin()->key.clone();

    // merge message buffer into leaf
    RecordBuckets res(tree_->options_.leaf_node_bucket_size);

    MsgBuf::Iterator it = mb->begin();
    RecordBuckets::Iterator jt = records_.get_iterator();
    while (it != mb->end() && jt.valid()) {
        int n = tree_->options_.comparator->compare(it->key, jt.record().key);
        if (n < 0) {
            if (it->type == Put) {
                res.push_back(to_record(*it));
            } else {
                // just throw deletion to non-exist record
                it->destroy();
            }
            it ++;
        } else if (n > 0) {
            res.push_back(jt.record());
            jt.next();
        } else {
            if (it->type == Put) {
                res.push_back(to_record(*it));
            }
            // old record is deleted
            it ++;
            jt.record().key.destroy();
            jt.record().value.destroy();
            jt.next();
        }
    }
    for (; it != mb->end(); it++) {
        if (it->type == Put) {
            res.push_back(to_record(*it));
        }
    }
    while(jt.valid()) {
        res.push_back(jt.record());
        jt.next();
    }
    records_.swap(res);

    refresh_buckets_info();
    set_dirty(true);

    // clear message buffer
    mb->clear();
    parent->msgcnt_ = parent->msgcnt_ + mb->count() - oldcnt;
    parent->msgbufsz_ = parent->msgbufsz_ + mb->size() - oldsz;

    // unlock message buffer
    mb->unlock();
    // crab walk
    parent->unlock();

    if (records_.size() == 0) {
        merge(anchor);
    } else if (records_.size() > 1 && (records_.size() > 
        tree_->options_.leaf_node_record_count || size() > 
        tree_->options_.leaf_node_page_size)) {
        split(anchor);
    } else {
        unlock();
    }
    
    anchor.destroy();
    return true;
}

Record LeafNode::to_record(const Msg& m)
{
    assert(m.type == Put);
    return Record(m.key, m.value);
}


void LeafNode::split(Slice anchor)
{
    if (balancing_) {
        unlock();
        return;
    }
    balancing_ = true;
    assert(records_.size() > 1);
    // release the write lock
    unlock();

    // need to search from root to leaf again
    // since the path may be modified
    vector<DataNode*> path;
    tree_->lock_path(anchor, path);
    assert(path.back() == this);

    // may have deletions during this period
    if (records_.size() <= 1 ||
        (records_.size() <= (tree_->options_.leaf_node_record_count / 2) &&
         size() <= (tree_->options_.leaf_node_page_size / 2) )) {
        while (path.size()) {
            path.back()->unlock();
            path.back()->dec_ref();
            path.pop_back();
        }
        return;
    }
   
    // create new leaf
    LeafNode *nl = tree_->new_leaf_node();
    assert(nl);

    // set siblings
    nl->left_sibling_ = nid_;
    nl->right_sibling_ = right_sibling_;
    if(right_sibling_ >= NID_LEAF_START) {
        LeafNode *rl = (LeafNode*)tree_->load_node(right_sibling_, false);
        assert(rl);
        rl->write_lock();
        rl->left_sibling_ = nl->nid_;
        rl->set_dirty(true);
        rl->unlock();
        rl->dec_ref();
    }
    right_sibling_ = nl->nid_;

    Slice k = records_.split(nl->records_);
    refresh_buckets_info();
    nl->refresh_buckets_info();

    set_dirty(true);
    nl->set_dirty(true);
    nl->dec_ref();

    balancing_ = false;
    path.pop_back();
    unlock();
    dec_ref();

    // propagation
    InnerNode *parent = (InnerNode*) path.back();
    assert(parent);
    parent->add_pivot(k, nl->nid_, path);
}

void LeafNode::merge(Slice anchor)
{
    if (balancing_) {
        unlock();
        return;
    }
    balancing_ = true;
    assert(records_.size() == 0);
    // release the write lock
    unlock();

    // acquire write locks from root to leaf
    vector<DataNode*> path;
    tree_->lock_path(anchor, path);
    assert(path.back() == this);

    // may have insertions during this period
    if (records_.size() > 0) {
        while (path.size()) {
            path.back()->unlock();
            path.back()->dec_ref();
            path.pop_back();
        }
        return;
    }
    
    if (left_sibling_ >= NID_LEAF_START) {
        LeafNode *ll = (LeafNode*)tree_->load_node(left_sibling_, false);
        assert(ll);
        ll->write_lock();
        ll->right_sibling_ = right_sibling_;
        ll->set_dirty(true);
        ll->unlock();
        ll->dec_ref();
    }
    if (right_sibling_ >= NID_LEAF_START) {
        LeafNode *rl = (LeafNode*)tree_->load_node(right_sibling_, false);
        assert(rl);
        rl->write_lock();
        rl->left_sibling_ = left_sibling_;
        rl->set_dirty(true);
        rl->unlock();
        rl->dec_ref();
    }
    dead_ = true;
    balancing_ = false;

    path.pop_back();
    unlock();
    dec_ref();

    // propagation
    InnerNode *parent = (InnerNode*) path.back();
    assert(parent);
    parent->rm_pivot(nid_, path);
}

bool LeafNode::find(Slice key, Slice& value, InnerNode *parent) 
{
    assert(parent);
    read_lock();

    parent->unlock();

    size_t idx = 0;
    for (; idx < buckets_info_.size(); idx ++) {
        if (tree_->options_.comparator->compare(key, buckets_info_[idx].key) < 0) {
            break;
        }
    }

    if (idx == 0) {
        unlock();
        return false;
    }

    RecordBucket *bucket = records_.bucket(idx - 1);
    if (bucket == NULL) {
        if (!load_bucket(idx - 1)) {
            LOG_ERROR("load bucket error nid " << nid_ << ", bucket " << (idx-1));
            unlock();
            return false;
        }
        bucket = records_.bucket(idx - 1);
        assert(bucket);
    } 

    bool ret = false;
    vector<Record>::iterator it = lower_bound(
        bucket->begin(), bucket->end(), key, KeyComp(tree_->options_.comparator));
    if (it != bucket->end() && it->key == key) {
        ret = true;
        value = it->value.clone();
    }

    unlock();
    return ret;
}

void LeafNode::lock_path(Slice key, std::vector<DataNode*>& path)
{
}

size_t LeafNode::size()
{
    return 8 + 8 + buckets_info_size_ + records_.length();
}

size_t LeafNode::estimated_buffer_size()
{
    size_t length = 8 + 8 + buckets_info_size_;
    if (tree_->compressor_) { 
        for (size_t i = 0; i < records_.buckets_number(); i++) {
            length += tree_->compressor_->max_compressed_length(records_.bucket_length(i));
        }
    } else {
        length += records_.length();
    }
    return length;
}

bool LeafNode::read_from(BlockReader& reader, bool skeleton_only)
{
    if (!reader.readUInt64(&left_sibling_)) return false;
    if (!reader.readUInt64(&right_sibling_)) return false;

    if (!read_buckets_info(reader)) {
        LOG_ERROR("read buckets info error, nid " << nid_);
        return false;
    }

    if (!skeleton_only) {
        if (!load_all_buckets(reader)) {
            LOG_ERROR("read all records bucket error, nid " << nid_);
            return false;
        }
    }
    return true;
}

bool LeafNode::write_to(BlockWriter& writer, size_t& skeleton_size)
{
    assert(status_ == kNew || status_ == kFullLoaded);

    size_t skeleton_pos = writer.pos();
    skeleton_size = 8 + 8 + buckets_info_size_;
    if (!writer.skip(skeleton_size)) return false;

    Slice buffer;
    if (tree_->compressor_) {
        size_t buffer_length = 0;
        for (size_t i = 0; i < records_.buckets_number(); i++) {
            if (buffer_length < records_.bucket_length(i)) {
                buffer_length = records_.bucket_length(i);
            }
        }
        buffer = Slice::alloc(buffer_length);
    }

    assert(records_.buckets_number() == buckets_info_.size());
    for (size_t i = 0; i < records_.buckets_number(); i++ ) {
        RecordBucket* bucket = records_.bucket(i);
	char *bkt_buffer = writer.addr();

        buckets_info_[i].offset = writer.pos();
        if (!write_bucket(writer, bucket, buffer)) {
            if (buffer.size()) {
                buffer.destroy();
            }
            return false;
        }
        buckets_info_[i].length = writer.pos() - buckets_info_[i].offset;
        buckets_info_[i].uncompressed_length = records_.bucket_length(i);
        buckets_info_[i].crc = crc16(bkt_buffer, buckets_info_[i].length);
    }
    size_t last_pos = writer.pos();

    if (buffer.size()) {
        buffer.destroy();
    }

    writer.seek(skeleton_pos);
    if (!writer.writeUInt64(left_sibling_)) return false;
    if (!writer.writeUInt64(right_sibling_)) return false;

    if (!write_buckets_info(writer)) {
        LOG_ERROR("write buckets info error, nid " << nid_);
        return false;
    }
    writer.seek(last_pos);
    return true;
}

bool LeafNode::write_bucket(BlockWriter& writer, RecordBucket *bucket, Slice buffer)
{
    if (tree_->compressor_) {
        // 1. write to buffer
        Block block(buffer, 0, 0);
        BlockWriter wr(&block);
        if (!write_bucket(wr, bucket)) return false;

        // 2. compress
        assert(tree_->compressor_->max_compressed_length(block.size()) <=
            writer.remain());

        size_t compressed_length;
        if (!tree_->compressor_->compress(buffer.data(), block.size(),
             writer.addr(), &compressed_length)) {
            LOG_ERROR("compress msgbuf error, nid " << nid_);
            return false;
        }

        // 3. skip
        writer.skip(compressed_length);

        return true;
    } else {
        return write_bucket(writer, bucket);
    }
}

bool LeafNode::write_bucket(BlockWriter& writer, RecordBucket *bucket)
{
    if (!writer.writeUInt32(bucket->size())) return false;
    for (size_t j = 0; j < bucket->size(); j++) {
        if (!(*bucket)[j].write_to(writer)) return false;
    }
    return true;
}

void LeafNode::refresh_buckets_info()
{
    // clean old info
    for (size_t i = 0; i < buckets_info_.size(); i++) {
        buckets_info_[i].key.destroy();
    }

    buckets_info_size_ = 4;
    buckets_info_.resize(records_.buckets_number());
    for (size_t i = 0; i < records_.buckets_number(); i++) {
        RecordBucket *bucket = records_.bucket(i);
        assert(bucket);
        assert(bucket->size());

        buckets_info_[i].key = bucket->front().key.clone();
        buckets_info_[i].offset = 0;
        buckets_info_[i].length = 0;
        buckets_info_[i].uncompressed_length = 0;
        buckets_info_[i].crc = 0;

        buckets_info_size_ += 4 + buckets_info_[i].key.size()
		+ 4 // sizeof(offset)
		+ 4 // sizeof(length)
		+ 4 // sizeof(uncomoressed_length)
		+ 2;// sizeof(crc)
    }
}

bool LeafNode::read_buckets_info(BlockReader& reader)
{
    uint32_t nbuckets;
    if (!reader.readUInt32(&nbuckets)) return false;
    buckets_info_.resize(nbuckets);

    buckets_info_size_ = 4;
    for (size_t i = 0; i < nbuckets; i++ ) {
        if (!reader.readSlice(buckets_info_[i].key)) return false;
        if (!reader.readUInt32(&(buckets_info_[i].offset))) return false;
        if (!reader.readUInt32(&(buckets_info_[i].length))) return false;
        if (!reader.readUInt32(&(buckets_info_[i].uncompressed_length))) return false;
        if (!reader.readUInt16(&(buckets_info_[i].crc))) return false;
        buckets_info_size_ += 4 + buckets_info_[i].key.size() 
		+ 4 // sizeof(offset)
		+ 4 // sizeof(length)
		+ 4 // sizeof(uncomoressed_length)
		+ 2;// sizeof(crc)
    }

    // init buckets number
    records_.set_buckets_number(nbuckets);

    status_ = kSkeletonLoaded;
    return true;
}

bool LeafNode::write_buckets_info(BlockWriter& writer)
{
    if (!writer.writeUInt32(buckets_info_.size())) return false;
    for (size_t i = 0; i < buckets_info_.size(); i++ ) {
        if (!writer.writeSlice(buckets_info_[i].key)) return false;
        if (!writer.writeUInt32(buckets_info_[i].offset)) return false;
        if (!writer.writeUInt32(buckets_info_[i].length)) return false;
        if (!writer.writeUInt32(buckets_info_[i].uncompressed_length)) return false;
        if (!writer.writeUInt16(buckets_info_[i].crc)) return false;
    }
    return true;
}

bool LeafNode::load_bucket(size_t idx)
{
    assert(status_ != kFullLoaded);
    assert(idx < buckets_info_.size());
    assert(records_.bucket(idx) == NULL);

    uint32_t offset = buckets_info_[idx].offset;
    uint32_t length = buckets_info_[idx].length;
    uint32_t uncompressed_length = buckets_info_[idx].uncompressed_length;

    Block* block = tree_->layout_->read(nid_, offset, length);
    if (block == NULL) {
        LOG_ERROR("read bucket error " << " nid " << nid_ << ", idx " << idx
            << ", offset " << offset << ", length " << length);
        return false;
    }
    
    // do bucket crc checking
    uint16_t expected_crc = buckets_info_[idx].crc;
    uint16_t actual_crc = crc16(block->start(), length);
    
    if (expected_crc != actual_crc) {
        LOG_ERROR("bucket crc checking error " << " nid " << nid_ << ", idx " << idx
            << ", offset " << offset << ", length " << length
            << ", expected_crc " << expected_crc << " ,actual_crc " << actual_crc);

        tree_->layout_->destroy(block);
        return false;
    }

    BlockReader reader(block);

    RecordBucket *bucket = new RecordBucket();
    if (bucket == NULL) {
        tree_->layout_->destroy(block);
        return false;
    }

    Slice buffer;
    if (tree_->compressor_) {
        buffer = Slice::alloc(uncompressed_length);
    }

    if (!read_bucket(reader, length, uncompressed_length, bucket, buffer)) {
        if (buffer.size()) {
            buffer.destroy();
        }
        delete bucket;
        tree_->layout_->destroy(block);
        return false;
    }

    if (buffer.size()) {
        buffer.destroy();
    }

    // this operation must be inside read lock

    // lazy load, upgrade lock to write lock
    // TODO: write a upgradable rwlock
    unlock();
    write_lock();
    if (records_.bucket(idx) == NULL) {
        records_.set_bucket(idx, bucket);
    } else {
        // it's possible another read thread loading 
        // the same block at the same time
        delete bucket;
    }
    unlock();
    read_lock();

    tree_->layout_->destroy(block);
    return true;
}

bool LeafNode::load_all_buckets()
{
    // skeleton has already been loaded
    assert(status_ == kSkeletonLoaded);

    Block* block = tree_->layout_->read(nid_, false);
    if (block == NULL) {
        LOG_ERROR("read node error " << " nid " << nid_);
        return false;
    }

    // this operation must be inside write lock
    BlockReader reader(block);
    if (!load_all_buckets(reader)) {
        tree_->layout_->destroy(block);
        return false;
    }

    tree_->layout_->destroy(block);
    return true;
}

bool LeafNode::load_all_buckets(BlockReader& reader)
{
    Slice buffer;
    if (tree_->compressor_) {
        size_t buffer_length = 0;
        for (size_t i = 0; i < buckets_info_.size(); i++ ) {
            if (buffer_length < buckets_info_[i].uncompressed_length) {
                buffer_length = buckets_info_[i].uncompressed_length;
            }
        }
        buffer = Slice::alloc(buffer_length);
    }

    bool ret = true;
    for (size_t i = 0; i < buckets_info_.size(); i++) {
        reader.seek(buckets_info_[i].offset);

        RecordBucket *bucket = new RecordBucket();
        if (bucket == NULL) {
            ret = false;
            break;
        }

        if (!read_bucket(reader, buckets_info_[i].length, 
                         buckets_info_[i].uncompressed_length,
                         bucket, buffer)) {
            ret = false;
            delete bucket;
            break;
        }

        records_.set_bucket(i, bucket);
    }

    if (buffer.size()) {
        buffer.destroy();
    }

    status_ = kFullLoaded;
    return ret;
}

bool LeafNode::read_bucket(BlockReader& reader, 
                           size_t compressed_length,
                           size_t uncompressed_length,
                           RecordBucket *bucket, Slice buffer)
{
    if (tree_->compressor_) {
        // 1. uncompress
        assert(compressed_length <= reader.remain());
        assert(uncompressed_length <= buffer.size());

        // 1. uncompress
        if (!tree_->compressor_->uncompress(reader.addr(),
            compressed_length, (char *)buffer.data())) {
            return false;
        }
        reader.skip(compressed_length);

        // 2. deserialize
        Block block(buffer, 0, uncompressed_length);
        BlockReader rr(&block);
        return read_bucket(rr, bucket);
    } else {
        return read_bucket(reader, bucket);
    }
}

bool LeafNode::read_bucket(BlockReader& reader,
                           RecordBucket *bucket)
{
    uint32_t nrecords;
    if (!reader.readUInt32(&nrecords)) return false;

    bucket->resize(nrecords);
    for (size_t i = 0; i < nrecords; i++) {
        if (!(*bucket)[i].read_from(reader)) {
            return false;
        }
    }

    return true;
}
