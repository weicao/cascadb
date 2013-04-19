// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_CRC16_H_
#define CASCADB_UTIL_CRC16_H_

#include <stddef.h>
#include <stdint.h>

namespace cascadb {
    uint16_t crc16(const char *buf, uint32_t n);
}

#endif
