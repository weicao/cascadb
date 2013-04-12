// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_NODE_H_
#define CASCADB_NODE_H_

#include <vector>
#include <set>

#include "cascadb/slice.h"
#include "cascadb/comparator.h"
#include "serialize/layout.h"
#include "msg.h"
#include "record.h"

namespace cascadb {

#define NID_NIL             ((bid_t)0)
#define NID_SCHEMA          ((bid_t)1)
#define NID_START           (NID_NIL + 2)
#define NID_LEAF_START      (bid_t)((1LL << 48) + 1)
#define IS_LEAF(nid)        ((nid) >= NID_LEAF_START)

class Tree;

class Pivot {
public:
    Pivot() {}
    
    Pivot(Slice k, bid_t c, MsgBuf* mb)
    : key(k), child(c), msgbuf(mb)
    {
    }
    
    Slice       key;
    bid_t       child;

    // msgbuf, NULL if not loaded yet
    MsgBuf      *msgbuf;
    // offset of msgbuf in block
    uint32_t    offset;
    // length of msgbuf in block
    uint32_t    length;
    // length of msgbuf before compression
    uint32_t    uncompressed_length;
    // crc of msgbuf
    uint16_t    crc;
};

class Node {
public:
    Node(const std::string& table_name, bid_t nid)
    : table_name_(table_name), nid_(nid)
    {
        dirty_ = false;
        dead_ = false;
        flushing_ = false;
        
        refcnt_ = 0;
        pincnt_ = 0;
    }
    
    virtual ~Node() {}
    
    // size of node in memory
    virtual size_t size() = 0;

    // size of node after serialization
    virtual size_t estimated_buffer_size() = 0;

    virtual bool read_from(BlockReader& reader, bool skeleton_only) = 0;

    virtual bool write_to(BlockWriter& writer, size_t& skeleton_size) = 0;

    /***************************
         setter and getters
    ****************************/
    
    bid_t nid() 
    {
        return nid_;
    }

    const std::string& table_name()
    {
        return table_name_;
    }

    void set_dirty(bool dirty)
    {
        ScopedMutex lock(&mtx_);
        if (!dirty_ && dirty) {
            first_write_timestamp_ = now();
        }
        dirty_ = dirty;
    }
    
    bool is_dirty()
    {
        ScopedMutex lock(&mtx_);
        return dirty_;
    }
    
    void set_dead()
    {
        ScopedMutex lock(&mtx_);
        dead_ = true;
    }
    
    bool is_dead()
    {
        ScopedMutex lock(&mtx_);
        return dead_;
    }
    
    void set_flushing(bool flushing)
    {
        ScopedMutex lock(&mtx_);
        flushing_ = flushing;
    }
    
    bool is_flushing()
    {
        ScopedMutex lock(&mtx_);
        return flushing_;
    }
    
    Time get_first_write_timestamp()
    {
        ScopedMutex lock(&mtx_);
        return first_write_timestamp_;
    }
    
    Time get_last_used_timestamp()
    {
        ScopedMutex lock(&mtx_);
        return last_used_timestamp_;
    }
    
    /***************************
        countings/lock/latch
    ****************************/
    
    void inc_ref()
    {
        ScopedMutex lock(&mtx_);
        refcnt_ ++;
    }
    
    void dec_ref()
    {
        ScopedMutex lock(&mtx_);
        refcnt_ --;
        assert(refcnt_ >= 0);
        last_used_timestamp_ = now();
    }
    
    int ref()
    {
        ScopedMutex lock(&mtx_);
        return refcnt_;
    }
    
    void inc_pin()
    {
        ScopedMutex lock(&mtx_);
        pincnt_ ++;
    }
    
    void dec_pin()
    {
        ScopedMutex lock(&mtx_);
        pincnt_ --;
        assert(pincnt_ >= 0);
    }
    
    int pin()
    {
        ScopedMutex lock(&mtx_);
        return pincnt_;
    }
    
    // Read lock is locked when:
    // 1) inner node is being written or read
    // 2) leaf node is being read
    void read_lock()
    {
        lock_.read_lock();
    }

    bool try_read_lock()
    {
        return lock_.try_read_lock();
    }

    // Write lock is locked when:
    // 1) inner node is spliting or merging
    // 2) leaf node is being written
    // 3) node is flushed out
    void write_lock()
    {
        lock_.write_lock();
    }

    bool try_write_lock()
    {
        return lock_.try_write_lock();
    }
    
    void unlock()
    {
        lock_.unlock();
    }
    
protected:
    std::string     table_name_;
    bid_t           nid_;

    // TODO: use atomic
    Mutex           mtx_;
        
    bool            dirty_;
    bool            dead_;

    // flushing_ flag will protect the same node being 
    // written out concurrently
    bool            flushing_;

    // the order of dirty nodes be flushed out
    Time            first_write_timestamp_;

    // the order of clean nodes be evicted
    Time            last_used_timestamp_;
    
    // reference counting, node can be destructed only
    // when this count reaches 0
    int             refcnt_;

    // the number of times a node has been pinned, 
    // a node should not be flushed out when pinned
    int             pincnt_;

    // latch
    RWLock          lock_;
};

class SchemaNode : public Node {
public:
    SchemaNode(const std::string& table_name)
    : Node(table_name, NID_SCHEMA)
    {
        root_node_id = NID_NIL;
        next_inner_node_id = NID_NIL;
        next_leaf_node_id = NID_NIL;
        tree_depth = 0;
    }

    size_t size();

    size_t estimated_buffer_size();
    
    bool read_from(BlockReader& reader, bool skeleton_only);
    
    bool write_to(BlockWriter& writer, size_t& skeleton_size);

    bid_t           root_node_id;
    bid_t           next_inner_node_id;
    bid_t           next_leaf_node_id;
    size_t          tree_depth;
};

enum NodeStatus {
    kNew,
    kUnloaded,
    kSkeletonLoaded,
    kFullLoaded
};

class InnerNode;

class DataNode : public Node {
public:
    DataNode(const std::string& table_name, bid_t nid, Tree *tree)
    : Node(table_name, nid), tree_(tree), status_(kNew)
    {
    }

    virtual ~DataNode() {};

    // Merge messages cascading from parent
    virtual bool cascade(MsgBuf *mb, InnerNode* parent) = 0;

    // Find values buffered in this node and all descendants
    virtual bool find(Slice key, Slice& value, InnerNode* parent) = 0;
    
    virtual void lock_path(Slice key, std::vector<DataNode*>& path) = 0;

protected:
    Tree            *tree_;

    NodeStatus      status_;
};

class LeafNode;

class InnerNode : public DataNode {
public:
    InnerNode(const std::string& table_name, bid_t nid, Tree *tree)
    : DataNode(table_name, nid, tree),
      bottom_(false),
      first_child_(NID_NIL),
      first_msgbuf_(NULL),
      first_msgbuf_offset_(0),
      first_msgbuf_length_(0),
      first_msgbuf_uncompressed_length_(0),
      pivots_sz_(0),
      msgcnt_(0), 
      msgbufsz_(0)
    {
        assert(nid >= NID_START && nid < NID_LEAF_START);
    }
    
    virtual ~InnerNode();

    // called for newly created root node only
    void init_empty_root();
    
    bool put(Slice key, Slice value)
    {
        return write(Msg(Put, key.clone(), value.clone()));
    }
    
    bool del(Slice key)
    {
        return write(Msg(Del, key.clone()));
    }

    virtual bool cascade(MsgBuf *mb, InnerNode* parent);
    
    virtual bool find(Slice key, Slice& value, InnerNode* parent);
    
    void add_pivot(Slice key, bid_t nid, std::vector<DataNode*>& path);
    
    void rm_pivot(bid_t nid, std::vector<DataNode*>& path);

    size_t pivot_size(Slice key);
    
    size_t size();

    size_t estimated_buffer_size();
    
    bool read_from(BlockReader& reader, bool skeleton_only);
    
    bool write_to(BlockWriter& writer, size_t& skeleton_size);

    void lock_path(Slice key, std::vector<DataNode*>& path);
    
protected:
    friend class LeafNode;

    bool write(const Msg& m);
    int comp_pivot(Slice k, int i);
    int find_pivot(Slice k);
    
    MsgBuf* msgbuf(int idx);

    bid_t child(int idx);
    void set_child(int idx, bid_t c);
    
    void insert_msgbuf(const Msg& m, int idx);
    void insert_msgbuf(MsgBuf::Iterator begin, MsgBuf::Iterator end, int idx);
    
    int find_msgbuf_maxcnt();
    int find_msgbuf_maxsz();

    void maybe_cascade();
    
    void split(std::vector<DataNode*>& path);

    bool load_msgbuf(int idx);
    bool load_all_msgbuf();
    bool load_all_msgbuf(BlockReader& reader);
    bool read_msgbuf(BlockReader& reader, 
                     size_t compressed_length,
                     size_t uncompressed_length,
                     MsgBuf *mb, Slice buffer);

    bool write_msgbuf(BlockWriter& writer, MsgBuf *mb, Slice buffer);

    // true if children're leaf nodes
    bool bottom_;

    bid_t first_child_;
    MsgBuf* first_msgbuf_;
    uint32_t first_msgbuf_offset_;
    uint32_t first_msgbuf_length_;
    uint32_t first_msgbuf_uncompressed_length_;
    uint16_t first_msgbuf_crc_;
    
    std::vector<Pivot> pivots_;

    size_t pivots_sz_; 
    size_t msgcnt_;
    size_t msgbufsz_;
};

class LeafNode : public DataNode { 
public:
    LeafNode(const std::string& table_name, bid_t nid, Tree *tree);
    
    ~LeafNode();

    virtual bool cascade(MsgBuf *mb, InnerNode* parent);
    
    virtual bool find(Slice key, Slice& value, InnerNode* parent);
    
    size_t size();
    
    size_t estimated_buffer_size();

    bool read_from(BlockReader& reader, bool skeleton_only);
    
    bool write_to(BlockWriter& writer, size_t& skeleton_size);

    void lock_path(Slice key, std::vector<DataNode*>& path);
    
protected:
    Record to_record(const Msg& msg);
  
    void split(Slice anchor);
    
    void merge(Slice anchor);
    
    // refresh buckets_info_ after buckets_ is modified
    void refresh_buckets_info();
    bool read_buckets_info(BlockReader& reader);
    bool write_buckets_info(BlockWriter& writer);

    bool load_bucket(size_t idx);
    bool load_all_buckets();
    bool load_all_buckets(BlockReader& reader);
    bool read_bucket(BlockReader& reader, 
                     size_t compressed_length,
                     size_t uncompressed_length,
                     RecordBucket *bucket, Slice buffer);
    bool read_bucket(BlockReader& reader,
                     RecordBucket *bucket);

    bool write_bucket(BlockWriter& writer, RecordBucket *bucket, Slice buffer);
    bool write_bucket(BlockWriter& writer, RecordBucket *bucket);

private:
    // either spliting or merging to get tree balanced
    bool                    balancing_;

    bid_t                   left_sibling_;
    bid_t                   right_sibling_;
    
    // records're divided into buckets,
    // each bucket can be loaded individually
    struct BucketInfo{
        Slice               key;
        uint32_t            offset;
        uint32_t            length;
        uint32_t            uncompressed_length;
        uint16_t            crc;
    };
    size_t                  buckets_info_size_;

    std::vector<BucketInfo> buckets_info_;

    RecordBuckets           records_;

};



}

#endif
