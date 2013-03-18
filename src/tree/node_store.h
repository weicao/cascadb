#ifndef _CASCADB_NODE_STORE_H_
#define _CASCADB_NODE_STORE_H_

#include "node.h"

namespace cascadb {

class NodeFactory {
public:
    virtual Node* new_node(bid_t nid) = 0;
};

// Adapter to various implementation, e.g.
// There is a dead simple all-in-memory implementation 
// in purpose to testing Tree and Node, and another 
// sophisticated implementation with LRU cache which flushes
// dirty pages from time to time

class Layout;

class NodeStore {
public:
    virtual ~NodeStore() {}

    virtual bool init(NodeFactory *factory) = 0;

    // Put a newly created node into store
    // Caller should ensure Node's reference count to be zero,
    // and Node's reference count will be increase by one in 
    // this function. when used up, caller is responsible to
    // decrease the count by one
    virtual void put(bid_t nid, Node* node) = 0;

    // Get node from store
    // Node's reference count will be increased by one
    // in the function. when used up, caller is responsible to
    // decrease the count by one
    virtual Node* get(bid_t nid) = 0;

    // Flush all dirty pages
    virtual void flush() = 0;
};

}

#endif