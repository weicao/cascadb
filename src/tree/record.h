// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_TREE_RECORD_H_
#define CASCADB_TREE_RECORD_H_

#include <stdint.h>
#include <vector>
#include <stdexcept>

#include "cascadb/slice.h"
#include "serialize/block.h"

namespace cascadb {

// Data is stored as Record inside leaf node
class Record {
public:
    Record() {}
    Record(Slice k, Slice v) : key(k), value(v) {}

    size_t size();
    bool read_from(BlockReader& reader);
    bool write_to(BlockWriter& writer);
    
    Slice  key;
    Slice  value;
};

typedef std::vector<Record> RecordBucket;

// Records're arranged into multiple buckets inside a single leaf node,
// in CascaDB nodes're configured big enough to accelerate write speed,
// and allow each bucket be read, decompressed, and deserialized individually,
// so that point query can be faster and much more efficient.
// Length of each bucket can be configured in Options::leaf_node_bucket_size.
class RecordBuckets {
public:
    class Iterator {
    public:
        Iterator(RecordBuckets* container)
        : container_(container),
          bucket_idx_(0),
          record_idx_(0)
        {
        }

        bool valid()
        {
            if (bucket_idx_ == container_->buckets_number()) {
                return false;
            }
            return true;
        }

        void next()
        {
            RecordBucket* bucket = container_->bucket(bucket_idx_);
            assert(bucket);

            record_idx_ ++;
            if (record_idx_ == bucket->size()) {
                record_idx_ = 0;
                bucket_idx_ ++;
            }
        }

        Record& record() {
            RecordBucket* bucket = container_->bucket(bucket_idx_);
            assert(bucket && record_idx_ < bucket->size());
            return (*bucket)[record_idx_];
        }

    private:
        RecordBuckets   *container_;
        size_t          bucket_idx_;
        size_t          record_idx_;
    };

    RecordBuckets(size_t max_bucket_length)
    : max_bucket_length_(max_bucket_length),
     last_bucket_length_(0),
     length_(0),
     size_(0)
    {
    }

    ~RecordBuckets();

    size_t buckets_number()
    {
        return buckets_.size();
    }

    void set_buckets_number(size_t buckets_number)
    {
        assert(buckets_.size() == 0);
        buckets_.resize(buckets_number);
    }

    RecordBucket* bucket(size_t index)
    {
        assert(index < buckets_.size());
        return buckets_[index].bucket;
    }

    size_t bucket_length(size_t index)
    {
        assert(index < buckets_.size());
        return buckets_[index].length;
    }

    void set_bucket(size_t index, RecordBucket* bucket)
    {
        assert(index < buckets_.size());

        assert(buckets_[index].bucket == NULL);
        buckets_[index].bucket = bucket;

        buckets_[index].length = 4;
        for (size_t i = 0; i < bucket->size(); i++) {
            buckets_[index].length += (*bucket)[i].size();
        }
        
        length_ += buckets_[index].length;
        size_ += buckets_[index].bucket->size();
    }

    Iterator get_iterator() { return Iterator(this); }

    void push_back(Record record);

    inline size_t length() { return length_; }

    inline size_t size() { return size_; }

    // for writing test purpose only
    inline Record& operator[](size_t idx) {
        assert(idx < size_);
        for (size_t i = 0; i < buckets_.size(); i++ ) {
            RecordBucket *bucket = buckets_[i].bucket;
            if (idx < bucket->size()) {
                return (*bucket)[idx];
            } 
            idx -= buckets_[i].bucket->size();
        }
        throw std::runtime_error("access record buckets, bad idx");
    }

    void swap(RecordBuckets &other);

    Slice split(RecordBuckets &other);

private:
    size_t max_bucket_length_;

    struct RecordBucketInfo {
        RecordBucket    *bucket;
        size_t          length;
    };

    std::vector<RecordBucketInfo> buckets_;

    size_t last_bucket_length_;

    size_t length_;

    size_t size_;
};

}

#endif