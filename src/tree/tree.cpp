// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <vector>

#include "util/logger.h"
#include "tree.h"

using namespace std;
using namespace cascadb;

Tree::~Tree()
{
    if (root_) {
        root_->dec_ref();
    }

    if (schema_) {
        schema_->dec_ref();
    }

    delete node_factory_;
}

bool Tree::init()
{
    if(options_.comparator == NULL) {
        LOG_ERROR("no comparator set in options");
        return false;
    }

    node_factory_ = new TreeNodeFactory(this);
    if (!node_store_->init(node_factory_)) {
        LOG_ERROR("init node store error");
        return false;
    }

    schema_ = (SchemaNode*) node_store_->get(NID_SCHEMA);
    if (schema_ == NULL) {
        LOG_INFO("schema node doesn't exist, init empty db");
        schema_ = new SchemaNode(table_name_);
        schema_->root_node_id = NID_NIL;
        schema_->next_inner_node_id = NID_START;
        schema_->next_leaf_node_id = NID_LEAF_START;
        schema_->tree_depth = 2;
        schema_->set_dirty(true);
        node_store_->put(NID_SCHEMA, schema_);
    }

    if (schema_->root_node_id == NID_NIL) {
        LOG_INFO("root node doesn't exist, init empty");
        root_ = new_inner_node();
        root_->init_empty_root();

        schema_->write_lock();
        schema_->root_node_id = root_->nid();
        schema_->set_dirty(true);
        schema_->unlock();
    } else {
        LOG_INFO("load root node nid " << hex << schema_->root_node_id << dec);
        root_ = (InnerNode*)load_node(schema_->root_node_id);
    }

    assert(root_);
    return true;
}

bool Tree::put(Slice key, Slice value)
{
    assert(root_);
    InnerNode *root = root_;
    root->inc_ref();
    bool ret = root->put(key, value);
    root->dec_ref();
    return ret;
}

bool Tree::del(Slice key)
{
    assert(root_);
    InnerNode *root = root_;
    root->inc_ref();
    bool ret = root->del(key);
    root->dec_ref();
    return ret;
}

bool Tree::get(Slice key, Slice& value)
{
    assert(root_);
    InnerNode *root = root_;
    root->inc_ref();
    bool ret = root->find(key, value, NULL);
    root->dec_ref();
    return ret;
}

InnerNode* Tree::new_inner_node()
{
    schema_->write_lock();
    bid_t nid = schema_->next_inner_node_id ++;
    schema_->set_dirty(true);
    schema_->unlock();

    InnerNode* node = (InnerNode *)node_factory_->new_node(nid);
    assert(node);

    node_store_->put(nid, node);
    return node;
}

LeafNode* Tree::new_leaf_node()
{
    schema_->write_lock();
    bid_t nid = schema_->next_leaf_node_id ++;
    schema_->set_dirty(true);
    schema_->unlock();

    LeafNode* node = (LeafNode *)node_factory_->new_node(nid);
    assert(node);

    node_store_->put(nid, node);
    return node;
}

DataNode* Tree::load_node(bid_t nid)
{
    assert(nid != NID_NIL && nid != NID_SCHEMA);
    return (DataNode*) node_store_->get(nid);
}

void Tree::pileup(InnerNode *root)
{
    assert(root_ != root);
    root_->dec_ref();
    root_ = root;

    schema_->write_lock();
    schema_->root_node_id = root_->nid();
    schema_->tree_depth ++;
    schema_->set_dirty(true);
    schema_->unlock();
}

void Tree::collapse()
{
    root_->dec_ref();
    
    root_ = new_inner_node();
    root_->init_empty_root();
    assert(root_);

    schema_->write_lock();
    schema_->root_node_id = root_->nid();
    schema_->tree_depth  = 2;
    schema_->set_dirty(true);
    schema_->unlock();
}

void Tree::lock_path(Slice key, std::vector<DataNode*>& path)
{
    assert(root_);
    InnerNode *root = root_;
    root->inc_ref();
    root->write_lock();
    path.push_back(root);
    root->lock_path(key, path);
}

Tree::TreeNodeFactory::TreeNodeFactory(Tree *tree)
: tree_(tree)
{
}

Node* Tree::TreeNodeFactory::new_node(bid_t nid)
{
    if (nid == NID_SCHEMA) {
        return new SchemaNode(tree_->table_name_);
    } else {
        DataNode *node;
        if (nid >= NID_LEAF_START) {
            node = new LeafNode(tree_->table_name_, nid);
        } else {
            node = new InnerNode(tree_->table_name_, nid);
        }
        node->set_tree(tree_);
        return node;
    }
}