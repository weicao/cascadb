// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_BLOOM_H_
#define CASCADB_UTIL_BLOOM_H_


#include "cascadb/slice.h"

namespace cascadb {
    size_t bloom_size(int n);
    void bloom_create(const Slice* keys, int n, std::string* bitsets);
    bool bloom_matches(const Slice& k, const Slice& filter);
}

#endif
