// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
// leveldb, please bear with me ^_^

#include "bloom.h"

#define BIT_PER_KEY (12)
#define K (BIT_PER_KEY * 0.69) //0.69 =~ ln(2)

using namespace cascadb;

static inline uint32_t _hash(const char *key, size_t n, uint32_t seed)
{
    size_t i = 0;
    uint32_t h = seed;

    while (i < n) 
        h = ((h<< 5) + h) + (uint32_t)key[i++];  
    return h;
}

size_t cascadb::bloom_size(int n)
{
    size_t bits;
    size_t bytes;
    
    bits = BIT_PER_KEY * n;
    
    if (bits < 64) bits = 64;
    bytes = (bits + 7) / 8;

    // probes in filter
    bytes += 1; 

    return bytes;
}

void cascadb::bloom_create(const Slice* keys, int n, std::string* bitsets)
{
    size_t bits;
    size_t bytes;
    size_t init_size;
    char* array;
    
    bits = BIT_PER_KEY * n;
    
    if (bits < 64) bits = 64;
    bytes = (bits + 7) / 8;
    bits = bytes * 8;

    init_size = bitsets->size();
    bitsets->resize(init_size + bytes, 0);

    // remember # of probes in filter
    bitsets->push_back(static_cast<char>(K));  
    array = &(*bitsets)[init_size];

    for (size_t i = 0; i < (size_t)n; i++) {
        uint32_t h;
        uint32_t delta;
        uint32_t bitpos;

        h = _hash(keys[i].data(), keys[i].size(), 0xbc9f1d34);

        // rotate right 17 bits
        delta = (h >> 17) | (h << 15); 
        for (size_t j = 0; j < K; j++) {
            bitpos = h % bits;
            array[bitpos / 8] |= (1 << (bitpos % 8));
            h += delta;
        }
    }
}

bool cascadb::bloom_matches(const Slice& key, const Slice& filter)
{
    size_t k;
    size_t len;
    size_t bits;

    uint32_t h;
    uint32_t delta;
    uint32_t bitpos;

    const char* array;

    len = filter.size();
    if (len < 2) return false;

    array = filter.data();
    bits = (len - 1) * 8;

    h =  _hash(key.data(), key.size(), 0xbc9f1d34);
    // rotate right 17 bits
    delta = (h >> 17) | (h << 15);  

    k = array[len - 1];
    for (size_t j = 0; j < k; j++) {
        bitpos = h % bits;
        if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
        h += delta;
    }
    return true;
}
