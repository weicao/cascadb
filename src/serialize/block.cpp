// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "block.h"

using namespace cascadb;

bool BlockReader::readSlice(Slice& s)
{
    uint32_t sz;
    if (readUInt32(&sz)) {
        assert(offset_ <= block_->size_);
        if (offset_ + sz <= block_->size_) {
            s = Slice(block_->buf_ + offset_, sz).clone();
            offset_ += sz;
            return true;
        }
    }
    return false;
}

bool BlockWriter::writeSlice(Slice& s)
{
    size_t sz = s.size();
    if( writeUInt32(sz)) {
        assert(offset_ <= block_->limit_);
        if (offset_ + sz <= block_->limit_) {
            memcpy(block_->buf_ + offset_, s.data(), sz);
            offset_ += sz;
            if (offset_ > block_->size_) {
                block_->size_ = offset_;
            }
            return true;
        }
    }
    return false;
}
