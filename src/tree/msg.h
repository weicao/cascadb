// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_MSG_H_
#define _CASCADB_MSG_H_

#include <assert.h>

#include "cascadb/slice.h"
#include "cascadb/comparator.h"
#include "serialize/block.h"
#include "sys/sys.h"
#include "fast_vector.h"

#define FAST_VECTOR

// Implement message and message buffer

namespace cascadb {

// Messages're delayed write operations
enum MsgType {
    _Msg, // uninitialized state
    Put,
    Del,
};

class Msg {
public:
    Msg() : type(_Msg) {}
    
    Msg(MsgType t, Slice k, Slice v = Slice())
    : type(t), key(k), value(v)
    {
    }
    
    void set_put(Slice k, Slice v)
    {
        type = Put;
        key = k;
        value = v;
    }
    
    void set_del(Slice k)
    {
        type = Del;
        key = k;
    }
    
    size_t size() const;
    
    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);

    void destroy();

    MsgType type;
    Slice key;
    Slice value;
};

// Store all messages buffered for a child node
class MsgBuf {
public:
    MsgBuf(Comparator *comp)
    : comp_(comp), size_(0)
    {
    }
    
    ~MsgBuf();
    
    // Write a single Msg into MsgBuf
    void write(const Msg& msg);
    
    // Clear all Msg objects buffered but not destroy them
    void clear();
    
    // You should lock MsgBuf before use iterator related operations
    void lock()
    {
        mtx_.lock();
    }
    
    // unlock MsgBuf after iterator related operations
    void unlock()
    {
        mtx_.unlock();
    }

#ifdef FAST_VECTOR
    typedef FastVector<Msg> ContainerType;
#else
    typedef std::vector<Msg> ContainerType;
#endif
    
    typedef ContainerType::iterator Iterator;
    
    Iterator begin() { return container_.begin(); }
    
    Iterator end() { return container_.end(); }
    
    // Append range of Msg objects from another MsgBuf
    void append(Iterator first, Iterator last);

    // Find the whole buffer for the input key,
    // Return position of the first element no less than the input key,
    // aka the first element equal or bigger than the input key
    Iterator find(Slice key);
    
    // Return the number of messages buffered
    size_t count() const
    {
        return container_.size();
    }
    
    const Msg& get(size_t idx) const
    {
        assert(idx >= 0 && idx < container_.size());
        return container_[idx];
    }
    
    // Return space taken to store messages buffered
    size_t size() const
    {
        return 4 + size_;
    }

    bool read_from(BlockReader& reader);
    
    bool write_to(BlockWriter& writer);
    
private:
    Comparator          *comp_;
    mutable Mutex       mtx_;
    ContainerType       container_;
    size_t              size_;
};

}

#endif
