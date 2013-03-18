// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is copied from LevelDB and modifed a little 
// to add LevelDB style benchmark

#ifndef _CASCADB_BENCH_TESTUTIL_H_
#define _CASCADB_BENCH_TESTUTIL_H_

#include <string>
#include "cascadb/slice.h"
#include "random.h"

namespace cascadb {

// Store in *dst a random string of length "len" and return a Slice that
// references the generated data.
extern Slice RandomSlice(Random* rnd, size_t len, std::string* dst);

// Return a random key with the specified length that may contain interesting
// characters (e.g. \x00, \xff, etc.).
extern std::string RandomKey(Random* rnd, size_t len);

// Store in *dst a string of length "len" that will compress to
// "N*compressed_fraction" bytes and return a Slice that references
// the generated data.
extern Slice CompressibleSlice(Random* rnd, double compressed_fraction,
                                size_t len, std::string* dst);

}

#endif