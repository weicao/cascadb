#ifndef CASCADB_TEST_HELPER_H_
#define CASCADB_TEST_HELPER_H_

#define PUT(mb, k, v) \
{\
    (mb).write(Msg(Put, Slice(k).clone(), Slice(v).clone()));\
}

#define DEL(mb, k) \
{\
    (mb).write(Msg(Del, Slice(k).clone()));\
}

#define CHK_MSG(m, t, k, v) \
    EXPECT_EQ(t, (m).type);\
    EXPECT_EQ(k, (m).key);\
    EXPECT_EQ(v, (m).value);


#define CHK_REC(r, k, v)\
    EXPECT_EQ(k, (r).key);\
    EXPECT_EQ(v, (r).value);

#include "sys/sys.h"
#include "tree/tree.h"

namespace cascadb {

class TestNodeStore : public NodeStore {
public:
    ~TestNodeStore()
    {
        close();
    }

    bool init(NodeFactory* factory) {
        return true;
    }

    void put(bid_t nid, Node* node) {
        assert(node->ref() == 0);
        
        ScopedMutex lock(&mtx_);
        cache_[nid] = node;
        lock.unlock();
        
        node->inc_ref();
    }
    
    Node* get(bid_t nid) {
        Node* node = get_raw(nid);
        if (node) {
            node->inc_ref();
        }
        return node;
    }
    
    Node* get_raw(bid_t nid) {
        ScopedMutex lock(&mtx_);
        std::map<bid_t, Node*>::iterator it = cache_.find(nid);
        if (it != cache_.end()) {
            Node *node = it->second;
            lock.unlock();
            
            assert(node);
            return node;
        }
        return NULL;
    }
    
    void flush(bid_t nid) {}
    
    void flush() {}
    
    void close() {
        ScopedMutex lock(&mtx_);
        std::map<bid_t, Node*>::iterator it = cache_.begin();
        for(; it != cache_.end(); it++) {
            Node *node = it->second;
            assert(node->ref() == 0);
            delete node;
        }
        cache_.clear();
    }
    
private:
    Mutex                   mtx_;
    std::map<bid_t, Node*>  cache_;
};

}

#endif
