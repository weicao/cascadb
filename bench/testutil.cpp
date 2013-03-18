// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This file is copied from LevelDB and modifed a little 
// to add LevelDB style benchmark

#include "testutil.h"

#include <stdio.h>
#include <stdlib.h>

namespace cascadb {

Slice RandomSlice(Random* rnd, size_t len, std::string* dst) {
  dst->resize(len);
  for (size_t i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return Slice(*dst);
}

std::string RandomKey(Random* rnd, size_t len) {
  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {
    '\0', '\1', 'a', 'b', 'c', 'd', 'e', '\xfd', '\xfe', '\xff'
  };
  std::string result;
  for (size_t i = 0; i < len; i++) {
    result += kTestChars[rnd->Uniform(sizeof(kTestChars))];
  }
  return result;
}

extern Slice CompressibleSlice(Random* rnd, double compressed_fraction,
                                size_t len, std::string* dst) {
  size_t raw = static_cast<size_t>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomSlice(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return Slice(*dst);
}

}
