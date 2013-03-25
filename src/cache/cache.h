// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_CACHE_H_
#define CASCADB_CACHE_H_

#include "cascadb/options.h"
#include "tree/node.h"
#include "tree/node_store.h"
#include "serialize/block.h"
#include "serialize/layout.h"
#include "sys/sys.h"

#include <map>
#include <vector>

namespace cascadb {

// Node cache of fixed size
// When the percentage of dirty nodes reaches the high watermark, or get expired,
// they're flushed out in the order of timestamp node is modified for the first time.
// A reference count is maintained for each node, When cache is getting almost full,
// clean nodes would be evicted if their reference count drops to 0.
// Nodes're evicted in LRU order.
// Cache can be shared among multiple tables.

class Cache {
public:
    Cache(const Options& options);
    
    ~Cache();
    
    bool init();
    
    // Add a table to cache
    bool add_table(const std::string& tbn, NodeFactory *factory, Layout *layout);
    
    // Flush all dirty nodes in a table
    void flush_table(const std::string& tbn);

    // Delete a table from cache, all loaded nodes in the table're
    // destroied, dirty nodes're flushed by default
    void del_table(const std::string& tbn, bool flush = true);
    
    // Put newly created node into cache
    void put(const std::string& tbn, bid_t nid, Node* node);
    
    // Acquire node, if node doesn't exist in cache, load it from layout
    Node* get(const std::string& tbn, bid_t nid);
    
    // Write back dirty nodes if any condition satisfied,
    // Sweep out dead nodes
    void write_back();

    void debug_print(std::ostream& out);

protected:
    struct TableSettings {
        NodeFactory     *factory;
        Layout          *layout;
    };

    bool get_table_settings(const std::string& tbn, TableSettings& tbs);

    bool must_evict();

    // Test whether the cache grows larger than high watermark
    bool need_evict();

    // Evict least recently used clean nodes to make room
    void evict();

    void flush_nodes(std::vector<Node*>& nodes);

    struct WriteCompleteContext {
        Node            *node;
        Layout          *layout;
        Block           *block;
    };

    void write_complete(WriteCompleteContext* context, bool succ);

    void delete_nodes(std::vector<Node*>& nodes);
    
private:
    Options options_;

    RWLock tables_lock_;
    std::map<std::string, TableSettings> tables_;

    // TODO make me atomic
    Mutex size_mtx_;
    // total memory size occupied by nodes,
    // updated everytime the flusher thread runs
    size_t size_;   

    class CacheKey {
    public:
        CacheKey(const std::string& t, bid_t n): tbn(t), nid(n) {}

        bool operator<(const CacheKey& o) const {
            int c = tbn.compare(o.tbn);
            if (c < 0) { return true; }
            else if (c > 0) { return false; }
            else { return nid < o.nid; }
        }
        
        std::string tbn;
        bid_t nid;
    };

    RWLock nodes_lock_;

    std::map<CacheKey, Node*> nodes_;

    // ensure there is only one thread is doing evict/flush
    Mutex global_mtx_;
    
    bool alive_;
    // scan nodes not being used,
    // async flush dirty page out
    Thread* flusher_;
};

// Glue Cache and Tree
class CachedNodeStore : public NodeStore {
public:
    CachedNodeStore(Cache *cache, const std::string& table_name, Layout *layout);

    ~CachedNodeStore();

    bool init(NodeFactory *node_factory);

    void put(bid_t nid, Node* node);

    Node* get(bid_t nid);

    void flush();

private:
    Cache           *cache_;
    std::string     table_name_;
    Layout          *layout_;
};

}

#endif
