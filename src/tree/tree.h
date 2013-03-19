// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_TREE_H_
#define _CASCADB_TREE_H_

#include <assert.h>
#include <string>
#include <map>

#include "cascadb/slice.h"
#include "cascadb/comparator.h"
#include "cascadb/options.h"
#include "sys/sys.h"
#include "node.h"
#include "node_store.h"

namespace cascadb {

// Buffered B-Tree's roughly a B+-Tree except there is a buffer
// at each inner node.
// A write operation reaches the buffer at root node first and 
// returns immediately, and sometimes later when the buffer at root
// becomes full, it will be then flushed to the buffers among children.
// This process is repeated on an on, and write operations buffered
// inside a inner node always cascade to children when buffer become full,
// so finally the write arrives to leaf.
// Writes at Buffered B-Tree is optimized because flushing a buffer
// from parent to children is a batch write, that is to say, multiple
// buffered writes can be completed in a single disk write,
// it canbe proved writes in Buffered B-Tree is 10x-100x faster than
// trival B-Tree.
// Structure Modification Operations(SMO) like node split and merge
// are similar to tranditional B+-Tree implementation.

class Tree {
public:
    Tree(const std::string& table_name,
         const Options& options,
         NodeStore *node_store)
    : table_name_(table_name),
      options_(options),
      node_store_(node_store),
      node_factory_(NULL),
      schema_(NULL),
      root_(NULL)
    {
    }
    
    ~Tree();
    
    bool init();
    
    bool put(Slice key, Slice value);
    
    bool del(Slice key);

    bool get(Slice key, Slice& value);

private:
    friend class InnerNode;
    friend class LeafNode;

    InnerNode* new_inner_node();
    
    LeafNode* new_leaf_node();
    
    DataNode* load_node(bid_t nid);
    
    InnerNode* root() { return root_; }
    
    void pileup(InnerNode *root);
    
    void collapse();

    void lock_path(Slice key, std::vector<DataNode*>& path);

    class TreeNodeFactory : public NodeFactory {
    public:
        TreeNodeFactory(Tree *tree);
        Node* new_node(bid_t nid);
    private:
        Tree        *tree_;
    };

    std::string     table_name_;

    Options         options_;

    NodeStore       *node_store_;

    TreeNodeFactory *node_factory_;

    SchemaNode      *schema_;

    InnerNode       *root_;


};

}

#endif
