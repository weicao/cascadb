// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>

#include "util/logger.h"
#include "record.h"

using namespace std;
using namespace cascadb;

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

RecordBuckets::~RecordBuckets()
{
    for (size_t i = 0; i < buckets_.size(); i++) {
        delete buckets_[i].bucket;
    }
}

void RecordBuckets::push_back(Record record)
{
    RecordBucket* bucket;
    if (buckets_.size() == 0 || last_bucket_length_ + 
            record.size() > max_bucket_length_) {
        bucket = new RecordBucket();
        RecordBucketInfo info;
        info.bucket = bucket;
        info.length = 4;
        buckets_.push_back(info);
        last_bucket_length_ = info.length;
        length_ += info.length;
    } else {
        bucket = buckets_.back().bucket;
    }

    bucket->push_back(record);
    buckets_.back().length += record.size();
    last_bucket_length_ += record.size();
    
    length_ += record.size();
    size_ ++;
}

void RecordBuckets::swap(RecordBuckets &other)
{
    buckets_.swap(other.buckets_);
    std::swap(last_bucket_length_, other.last_bucket_length_);
    std::swap(length_, other.length_);
    std::swap(size_, other.size_);
}

Slice RecordBuckets::split(RecordBuckets &other)
{
    assert(other.buckets_number() == 0);
    assert(buckets_.size());

    if (buckets_.size() == 1) {
        // divide records in the first bucket
        RecordBucket *src = buckets_[0].bucket;
        RecordBucket *dst = new RecordBucket();

        size_t n = src->size() / 2;
        dst->resize(src->size() - n);
        std::copy(src->begin() + n, src->end(), dst->begin());
        src->resize(n);

        buckets_[0].length = 4;
        for (size_t i = 0; i < src->size(); i++) {
            buckets_[0].length += (*src)[i].size();
        }
        length_ = buckets_[0].length;
        size_ = src->size();

        other.set_buckets_number(1);
        other.set_bucket(0, dst);
    } else {
        // divide buckets
        size_t n = buckets_.size() / 2;
        other.set_buckets_number(buckets_.size() - n);
        for (size_t i = n; i < buckets_.size(); i++ ) {
            other.set_bucket(i-n, buckets_[i].bucket);
        }
        buckets_.resize(n);

        length_ -= other.length();
        size_ -= other.size();
    }

    assert(other.buckets_number());
    RecordBucket *bucket = other.bucket(0);
    assert(bucket->size());
    return bucket->front().key;
}