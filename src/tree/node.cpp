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

using namespace std;
using namespace cascadb;

/********************************************************
                        Pivot
*********************************************************/

size_t Pivot::size()
{
    return 4 + key.size() + 8 + msgbuf->size();
}

bool Pivot::read_from(BlockReader& reader)
{
    if (!reader.readSlice(key)) return false;
    if (!reader.readUInt64(&child)) return false;
    assert(msgbuf);
    if (!msgbuf->read_from(reader)) return false;
    return true;
}

bool Pivot::write_to(BlockWriter& writer)
{
    if (!writer.writeSlice(key)) return false;
    if (!writer.writeUInt64(child)) return false;
    assert(msgbuf);
    if (!msgbuf->write_to(writer)) return false;
    return true;
}

/********************************************************
                        Record
*********************************************************/

size_t Record::size()
{
    return 4 + key.size() + 4 + value.size();
}
        
bool Record::read_from(BlockReader& reader)
{
    if (!reader.readSlice(key)) return false;
    if (!reader.readSlice(value)) return false;
    return true;
}
    
bool Record::write_to(BlockWriter& writer)
{
    if (!writer.writeSlice(key)) return false;
    if (!writer.writeSlice(value)) return false;
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

    Slice k = m.key;
    write_msgbuf(m, find_pivot(k));
    set_dirty(true);
    
    maybe_cascade();
    return true;
}

bool InnerNode::cascade(MsgBuf *mb, InnerNode* parent){

    read_lock();

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
                write_msgbuf(rs, it, i);
                rs = it;
            }
            i ++;
        }
    }
    if(rs != end) {
        write_msgbuf(rs, end, i);
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
    if (idx == 0) {
        return first_msgbuf_;
    } else {
        return pivots_[idx-1].msgbuf;
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

void InnerNode::write_msgbuf(const Msg& m, int idx)
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

void InnerNode::write_msgbuf(MsgBuf::Iterator begin, 
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
        node = tree_->load_node(nid);
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

    vector<Pivot>::iterator it = std::lower_bound(pivots_.begin(), 
        pivots_.end(), key, KeyComp(tree_->options_.comparator));
    MsgBuf* mb = new MsgBuf(tree_->options_.comparator);
    pivots_.insert(it, Pivot(key.clone(), nid, mb));
    pivots_sz_ += pivot_size(key);
    msgbufsz_ += mb->size();
    
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
    
    set_dirty(true);
    ni->set_dirty(true);
    ni->dec_ref();

    path.pop_back();
    unlock();
    dec_ref();
    
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
}

void InnerNode::rm_pivot(bid_t nid, std::vector<DataNode*>& path)
{
    // todo free memory of pivot key

    assert(path.back() == this);

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
        for (vector<Pivot>::iterator it = pivots_.begin(); 
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
    MsgBuf* b = msgbuf(idx);
    assert(b);

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

    bid_t chidx = child(idx);
    if (chidx == NID_NIL) {
        assert(idx == 0); // must be the first child
        unlock();
        return false;
    }
    
    // find in child
    DataNode* ch = tree_->load_node(chidx);
    assert(ch);
    ret = ch->find(key, value, this);
    ch->dec_ref();
    return ret;
}

void InnerNode::lock_path(Slice key, std::vector<DataNode*>& path)
{
    int idx = find_pivot(key);
    DataNode* ch = tree_->load_node(child(idx));
    assert(ch);
    ch->write_lock();
    path.push_back(ch);
    ch->lock_path(key, path);
}

size_t InnerNode::pivot_size(Slice key)
{
    // 4 bytes for key size, 8 bytes for child nid size, and 1 byte for flag
    return 4 + key.size() + 8 + 1;
}

size_t InnerNode::size()
{
    size_t sz = 0;
    sz += 1; // bottom
    sz += 9; // first child nid and msgbuf flag
    sz += 4; // pivots number
    // for (size_t i = 0; i < pivots_.size(); i++) {
    //     sz += (4 + pivots_[i].key.size()); // pivot key
    //     sz += 9; // pivot child nid and msgbuf flag
    // }
    sz += pivots_sz_;
    sz += msgbufsz_;
    return sz;
}

bool InnerNode::read_from(BlockReader& reader)
{
    bool flag;
    if (!reader.readBool(&bottom_)) return false;
    if (!reader.readUInt64(&first_child_)) return false;
    if (!reader.readBool(&flag)) return false;
    if (flag) {
        first_msgbuf_ = new MsgBuf(tree_->options_.comparator);
        if (!first_msgbuf_->read_from(reader)) return false;
        msgcnt_ += first_msgbuf_->count();
        msgbufsz_ += first_msgbuf_->size();
    }
    uint32_t pn;
    if (!reader.readUInt32(&pn)) return false;
    pivots_.resize(pn);
    for (size_t i = 0; i < pn; i++) {
        if (!reader.readSlice(pivots_[i].key)) return false;
        pivots_sz_ += pivot_size(pivots_[i].key);
        if (!reader.readUInt64(&pivots_[i].child)) return false;
        if (!reader.readBool(&flag)) return false;
        if (flag) {
            pivots_[i].msgbuf = new MsgBuf(tree_->options_.comparator);
            if (!pivots_[i].msgbuf->read_from(reader)) return false;
            msgcnt_ += pivots_[i].msgbuf->count();
            msgbufsz_ += pivots_[i].msgbuf->size();
        }
    }
    return true;
}

bool InnerNode::write_to(BlockWriter& writer)
{
    if (!writer.writeBool(bottom_)) return false;
    if (!writer.writeUInt64(first_child_)) return false;
    if (first_msgbuf_) {
        if (!writer.writeBool(true)) return false;
        if (!first_msgbuf_->write_to(writer)) return false;
    } else {
        if (!writer.writeBool(false)) return false;
    }
    
    if (!writer.writeUInt32(pivots_.size())) return false;
    for (size_t i = 0; i < pivots_.size(); i++) {
        if (!writer.writeSlice(pivots_[i].key)) return false;
        if (!writer.writeUInt64(pivots_[i].child)) return false;
        if (pivots_[i].msgbuf) {
            if (!writer.writeBool(true)) return false;
            if (!pivots_[i].msgbuf->write_to(writer)) return false;
        } else {
            if (!writer.writeBool(false)) return false;
        }
    }
    return true;
}

/********************************************************
                        LeafNode
*********************************************************/

LeafNode::~LeafNode()
{
    for (size_t i = 0; i < records_.size(); i++ ) {
        records_[i].key.destroy();
        records_[i].value.destroy();
    }
    records_.clear();
}

bool LeafNode::cascade(MsgBuf *mb, InnerNode* parent)
{
    write_lock();

    // lock message buffer from parent
    mb->write_lock();
    size_t oldcnt = mb->count();
    size_t oldsz = mb->size();

    Slice anchor = mb->begin()->key.clone();

    // merge message buffer into leaf
    vector<Record> res;
    res.reserve(records_.size() + mb->count());
    MsgBuf::Iterator it = mb->begin();
    vector<Record>::iterator jt = records_.begin();
    while ( it != mb->end() && jt != records_.end()) {
        int n = tree_->options_.comparator->compare(it->key, jt->key);
        if (n < 0) {
            if (it->type == Put) {
                res.push_back(to_record(*it));
            } else {
                // just throw deletion to non-exist record
                it->destroy();
            }
            it ++;
        } else if (n > 0) {
            res.push_back(*jt);

            jt ++;
        } else {
            if (it->type == Put) {
                res.push_back(to_record(*it));
            }
            // old record is deleted
            jt->key.destroy();
            jt->value.destroy();
            it ++;
            jt ++;
        }
    }
    for (; it != mb->end(); it++) {
        if (it->type == Put) {
            res.push_back(to_record(*it));
        }
    }
    for (; jt != records_.end(); jt++) {
        res.push_back(*jt);
    }
    records_.swap(res);
    res.clear();
    recsz_ = calc_recsz();
    set_dirty(true);

    // clear message buffer
    mb->clear();
    parent->msgcnt_ = parent->msgcnt_ + mb->count() - oldcnt;
    parent->msgbufsz_ = parent->msgbufsz_ + mb->size() - oldsz;

    // unlock message buffer
    mb->unlock();
    // crab walk
    parent->unlock();

    if (records_.size() > 1 &&
        (records_.size() > tree_->options_.leaf_node_record_count ||
         size() > tree_->options_.leaf_node_page_size)) {
        split(anchor);
    } else if(records_.size() == 0) {
        merge(anchor);
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
    
    // select a pivot
    size_t n = records_.size()/2;
    Slice k = records_[n].key;
    
    // create new leaf
    LeafNode *nl = tree_->new_leaf_node();
    assert(nl);
    nl->write_lock();

    // set siblings
    nl->left_sibling_ = nid_;
    nl->right_sibling_ = right_sibling_;
    if(right_sibling_ >= NID_LEAF_START) {
        LeafNode *rl = (LeafNode*)tree_->load_node(right_sibling_);
        assert(rl);
        rl->write_lock();
        rl->left_sibling_ = nl->nid_;
        rl->set_dirty(true);
        rl->unlock();
        rl->dec_ref();
    }
    right_sibling_ = nl->nid_;

    // move records
    nl->records_.resize(records_.size() - n);
    std::copy(records_.begin() + n, records_.end(), nl->records_.begin());
    records_.resize(n);

    // set size
    nl->recsz_ = nl->calc_recsz();
    recsz_ -= nl->recsz_;
    
    set_dirty(true);
    nl->set_dirty(true);

    nl->unlock();
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

size_t LeafNode::calc_recsz()
{
    size_t sz = 0;
    for (vector<Record>::iterator it = records_.begin(); 
        it != records_.end(); it ++ ) {
        sz += it->size();
    }
    return sz;
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
        LeafNode *ll = (LeafNode*)tree_->load_node(left_sibling_);
        assert(ll);
        ll->write_lock();
        ll->right_sibling_ = right_sibling_;
        ll->set_dirty(true);
        ll->unlock();
        ll->dec_ref();
    }
    if (right_sibling_ >= NID_LEAF_START) {
        LeafNode *rl = (LeafNode*)tree_->load_node(right_sibling_);
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
    bool ret = false;
    assert(parent);
    read_lock();

    parent->unlock();

    vector<Record>::iterator it = lower_bound(
        records_.begin(), records_.end(), key, KeyComp(tree_->options_.comparator));
    if (it != records_.end() && it->key == key) {
        ret = true;
        value = it->value.clone();
    }

    unlock();
    return ret;
}

void LeafNode::lock_path(Slice key, std::vector<DataNode*>& path)
{
    return;
}

size_t LeafNode::size()
{
    return 8*2 + 4 + recsz_;
}

bool LeafNode::read_from(BlockReader& reader)
{
    if (!reader.readUInt64(&left_sibling_)) return false;
    if (!reader.readUInt64(&right_sibling_)) return false;
    
    uint32_t n;
    if (!reader.readUInt32(&n)) return false;
    records_.resize(n);
    for (size_t i = 0; i < n; i++) {
        if (!records_[i].read_from(reader)) return false;
    }
    recsz_ = calc_recsz();
    return true;
}

bool LeafNode::write_to(BlockWriter& writer)
{
    if (!writer.writeUInt64(left_sibling_)) return false;
    if (!writer.writeUInt64(right_sibling_)) return false;
    if (!writer.writeUInt32(records_.size())) return false;
    for (size_t i = 0; i < records_.size(); i++) {
        if (!records_[i].write_to(writer)) return false;
    } 
    return true;
}

size_t SchemaNode::size()
{
    return 32;
}

bool SchemaNode::read_from(BlockReader& reader)
{
    if (!reader.readUInt64(&root_node_id)) return false;
    if (!reader.readUInt64(&next_inner_node_id)) return false;
    if (!reader.readUInt64(&next_leaf_node_id)) return false;
    if (!reader.readUInt64(&tree_depth)) return false;

    return true;
}

bool SchemaNode::write_to(BlockWriter& writer)
{
    if (!writer.writeUInt64(root_node_id)) return false;
    if (!writer.writeUInt64(next_inner_node_id)) return false;
    if (!writer.writeUInt64(next_leaf_node_id)) return false;
    if (!writer.writeUInt64(tree_depth)) return false;

    return true;
}
