#ifndef _CASCADB_NODE_H_
#define _CASCADB_NODE_H_

#include <vector>
#include <set>

#include "cascadb/slice.h"
#include "cascadb/comparator.h"
#include "serialize/layout.h"
#include "msg.h"

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
    
    size_t size();
    
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);
    
    Slice  key;
    bid_t   child;
    MsgBuf  *msgbuf;
};

class Record {
public:
    Record() {}

    Record(Slice k, Slice v) 
    : key(k), value(v) {}

    size_t size();
        
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);
    
    Slice  key;
    Slice  value;
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
    
    virtual size_t size() = 0;
    
    virtual bool read_from(BlockReader& reader) = 0;

    virtual bool write_to(BlockWriter& writer) = 0;

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
    
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);

    bid_t           root_node_id;
    bid_t           next_inner_node_id;
    bid_t           next_leaf_node_id;
    size_t          tree_depth;
};

class InnerNode;
class LeafNode;

class DataNode : public Node {
public:
    DataNode(const std::string& table_name, bid_t nid)
    : Node(table_name, nid), tree_(NULL)
    {
    }

    virtual ~DataNode() {};

    // Merge messages cascading from parent
    virtual bool cascade(MsgBuf *mb, InnerNode* parent) = 0;

    // Find values buffered in this node and all descendants
    virtual bool find(Slice key, Slice& value, InnerNode* parent) = 0;
    
    virtual void lock_path(Slice key, std::vector<DataNode*>& path) = 0;

    // getters and setters

    void set_tree(Tree* tree)
    {
        tree_ = tree;
    }

    Tree* get_tree()
    {
        return tree_;
    }

protected:
    Tree            *tree_;
};

class InnerNode : public DataNode {
public:
    InnerNode(const std::string& table_name, bid_t nid)
    : DataNode(table_name, nid),
      bottom_(false),
      first_child_(NID_NIL),
      first_msgbuf_(NULL),
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
    
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);

    void lock_path(Slice key, std::vector<DataNode*>& path);
    
protected:
    friend class LeafNode;

    bool write(const Msg& m);
    int comp_pivot(Slice k, int i);
    int find_pivot(Slice k);
    
    MsgBuf* msgbuf(int idx);

    bid_t child(int idx);
    void set_child(int idx, bid_t c);
    
    void write_msgbuf(const Msg& m, int idx);
    void write_msgbuf(MsgBuf::Iterator begin, MsgBuf::Iterator end, int idx);
    
    int find_msgbuf_maxcnt();
    int find_msgbuf_maxsz();

    void maybe_cascade();
    
    void split(std::vector<DataNode*>& path);

    // true if children're leaf nodes
    bool bottom_;

    bid_t first_child_;
    MsgBuf* first_msgbuf_;
    
    std::vector<Pivot> pivots_;

    size_t pivots_sz_; 
    size_t msgcnt_;
    size_t msgbufsz_;
};

class LeafNode : public DataNode { 
public:
    LeafNode(const std::string& table_name, bid_t nid)
    : DataNode(table_name, nid),
      balancing_(false),
      left_sibling_(NID_NIL),
      right_sibling_(NID_NIL),
      recsz_(0)
    {
        assert(nid >= NID_LEAF_START);
    }
    
    ~LeafNode();
    
    virtual bool cascade(MsgBuf *mb, InnerNode* parent);
    
    virtual bool find(Slice key, Slice& value, InnerNode* parent);
    
    size_t size();
    
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);

    void lock_path(Slice key, std::vector<DataNode*>& path);
    
protected:
    Record to_record(const Msg& msg);
    
    void split(Slice anchor);
    
    void merge(Slice anchor);
    
    size_t calc_recsz();
    
private:
    // either spliting or merging to get tree balanced
    bool                    balancing_;

    bid_t                   left_sibling_;
    bid_t                   right_sibling_;
    
    std::vector<Record>     records_;
    size_t                  recsz_;
};



}

#endif
