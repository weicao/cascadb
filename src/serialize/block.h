// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_SERIALIZE_BLOCK_H_
#define CASCADB_SERIALIZE_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>

#include "cascadb/slice.h"
#include "util/bits.h"

namespace cascadb {

typedef uint64_t bid_t;

// Chunk of memory buffered/managed by Layout
class Block {
public:
    Block(Slice buf, size_t start, size_t size)
    : buf_(buf), start_(start), size_(size)
    {
        assert(start_ < buf_.size());
        assert(start_ + size_ <= buf_.size());
    }
    
    Slice& buffer() {return buf_;}

    size_t size() {return size_;}

    void set_size(size_t size) 
    {
        assert(start_ + size_ <= buf_.size());
        size_ = size;
    }

    inline const char* start()
    {
        return buf_.data() + start_;
    }

    inline size_t capacity()
    {
        return buf_.size() - start_;
    }

    inline size_t remain()
    {
        return capacity() - size_;
    }
    
    void clear() {size_ = 0;}
    
private:
    friend class BlockReader;
    friend class BlockWriter;
    
    Block(Block& o);  // usage denied
    Block& operator=(Block& o); // usage denied

    Slice buf_;     // buffer
    size_t start_;  // start offset in buffer
    size_t size_;   // size of buffer
};

// Read operations to Block
class BlockReader
{
public:
    BlockReader(Block* block)
    : block_(block), offset_(0)
    {
    }

    char *addr()
    {
        return (char *)block_->start() + pos();
    }
    
    void seek(size_t offset)
    {
        offset_ = offset;
    }
    
    size_t pos()
    {
        return offset_;
    }

    bool skip(size_t length)
    {
        if (offset_ + length <= block_->size_) {
            offset_ += length;
            return true;
        } else {
            return false;
        }
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
            *v = *(T*)(block_->start() + offset_);
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

    char *addr()
    {
        return (char *)block_->start() + pos();
    }
    
    void seek(size_t offset)
    {
        offset_ = offset;
    }

    size_t pos()
    {
        return offset_;
    }

    bool skip(size_t length)
    {
        if (offset_ + length <= block_->capacity()) {
            offset_ += length;
            if(block_->size_ < offset_) {
                block_->size_ = offset_;
            }
            return true;
        } else {
            return false;
        }
    }

    size_t remain()
    {
        return block_->capacity() - offset_;
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
        assert(offset_ <= block_->capacity());
        if (offset_ + sizeof(T) <= block_->capacity()) {
            *(T*)(block_->start() + offset_) = v;
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
