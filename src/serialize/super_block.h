// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_SERIALIZE_SUPER_BLOCK_H_
#define _CASCADB_SERIALIZE_SUPER_BLOCK_H_

#include "cascadb/options.h"
#include "block.h"

namespace cascadb {

#define SUPER_BLOCK_SIZE        4096

class BlockMeta;

class SuperBlock {
public:
    SuperBlock()
    {
        magic_number = 0x62646163736163;    // "cascadb"
        major_version = 0;                  // "version 0.1"
        minor_version = 1;

        compress = kNoCompress;
        index_block_meta = NULL;
        crc = 0;
    }

    uint64_t        magic_number;
    uint8_t         major_version;
    uint8_t         minor_version;

    Compress        compress;
    BlockMeta       *index_block_meta;
    uint16_t        crc;                    // crc of SuperBlock
};

}

#endif