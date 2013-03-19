// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_SERIALIZE_BLOCK_H_
#define _CASCADB_SERIALIZE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "cascadb/slice.h"

///@todo read/write into block & compression

namespace cascadb {

typedef uint64_t bid_t;

// Chunk of memory buffered/managed by Layout
class Block {
public:
    Block(Slice s, size_t size)
    : buf_((char*)(s.data())), size_(size), limit_(s.size())
    {
    }

    Block(char* b, size_t s, size_t l)
    : buf_(b), size_(s), limit_(l)
    {
    }
    
    char* buf() {return buf_;}
    void set_size(size_t size) {assert(size <= limit_); size_ = size;}
    size_t size() {return size_;}
    size_t limit() {return limit_;}
    size_t remain() {return limit_-size_;}
    
    void clear() {size_ = 0;}
    
private:
    friend class BlockReader;
    friend class BlockWriter;
    
    Block(Block& o);  // usage denied
    Block& operator=(Block& o); // usage denied

    char* buf_;     // buffer begin
    size_t size_;   // buffer size
    size_t limit_;  // maximum size
};

// Read operations to Block
class BlockReader
{
public:
    BlockReader(Block* block)
    : block_(block), offset_(0)
    {
    }
    
    void seek(size_t offset)
    {
        offset_ = offset;
    }
    
    size_t remain()
    {
        assert(offset_ <= block_->size_);
        return block_->size_ - offset_;
    }

    bool readBool(bool* v) { return readUInt((uint8_t*)v); }
    bool readUInt8(uint8_t* v) { return readUInt(v); }
    bool readUInt16(uint16_t* v) { return readUInt(v); }
    bool readUInt32(uint32_t* v) { return readUInt(v); }
    bool readUInt64(uint64_t* v) { return readUInt(v); }
    bool readSlice(Slice & s);
    
protected:
    template<typename T>
    bool readUInt(T* v) {
        assert(offset_ <= block_->size_);
        if (offset_ + sizeof(T) <= block_->size_) {
            *v = *(T*)(block_->buf_ + offset_);
            offset_ += sizeof(T);
            return true;
        } else {
            return false;
        }
    }
    
private:
    Block* block_;
    size_t offset_;
};

// Write operations to Block
class BlockWriter
{
public:
    BlockWriter(Block* block)
    : block_(block), offset_(0)
    {
    }
    
    void seek(size_t offset)
    {
        offset_ = offset;
    }

    size_t remain()
    {
        assert(offset_ <= block_->limit_);
        return block_->limit_ - offset_;
    }
    
    bool writeBool(bool v) {return writeInt(v);}
    bool writeUInt8(uint8_t v) { return writeInt(v); }
    bool writeUInt16(uint16_t v) { return writeInt(v); }
    bool writeUInt32(uint32_t v) { return writeInt(v); }
    bool writeUInt64(uint64_t v) { return writeInt(v); }
    bool writeSlice(Slice &s);

protected:
    template<typename T>
    bool writeInt(T v) {
        assert(offset_ <= block_->limit_);
        if (offset_ + sizeof(T) <= block_->limit_) {
            *(T*)(block_->buf_ + offset_) = v;
            offset_ += sizeof(T);
            if(block_->size_ < offset_) {
                block_->size_ = offset_;
            }
            return true;
        } else {
            return false;
        }
    }
    
    
private:
    Block* block_;
    size_t offset_;
};

}

#endif
