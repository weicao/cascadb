// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_OPTIONS_H_
#define CASCADB_OPTIONS_H_

#include <stddef.h>
#include <stdint.h>

namespace cascadb {

class Directory;
class Comparator;

enum Compress {
    kNoCompress,   // No compression
    kSnappyCompress  // Google's Snappy, used in leveldb
};

class Options {
public:
    // Set defaults
    Options() {
        dir = NULL;
        comparator = NULL;

        inner_node_page_size = 4<<20;       // 4M, bigger inner node improve write performance
                                            // but degrade read performance
        inner_node_children_number = 16;    // bigger fanout will decrease the number of inner nodes,
                                            // but degrade write performance
        leaf_node_page_size = 4<<20;        // 4M, smaller leaf improve read performance,
                                            // but increases the number of inner nodes
        leaf_node_bucket_size = 128<<10;    // leaf nodes 're divdided into serveral buckets,
                                            // bucket is the unit of disk read when do point queries,
                                            // smaller value favors point query but may decrease
                                            // compression ratio.
        inner_node_msg_count = -1;          // unlimited by default, leaved for writing unit test,
                                            // you should NOT use it
        leaf_node_record_count = -1;        // unlimited by default, leaved for writing unit test,
                                            // you should NOT use it
        cache_limit = 512 << 20;            // 512M, it's best to be set twice of the total size of inner nodes
        cache_dirty_high_watermark = 30;    // 30%
        cache_dirty_expire = 60000;         // 1 minute
        cache_writeback_ratio = 1;          // 1%
        cache_writeback_interval = 100;     // 100ms
        cache_evict_ratio = 1;              // 1%
        cache_evict_high_watermark = 95;    //95%

        compress = kNoCompress;
        check_crc = false;
    }

    /******************************
               Components
    ******************************/

    // Directory where data files and WAL store
    Directory *dir;

    // Key comparator
    Comparator *comparator;

    /******************************
        Buffered BTree Parameters
    ******************************/

    // Page size of inner node
    size_t inner_node_page_size;

    // Maximum children number of iner node
    size_t inner_node_children_number;

    // Page size of leaf node
    size_t leaf_node_page_size;

    // Bucket size inside leaf node
    size_t leaf_node_bucket_size;

    // Maximum count of buffered messages in InnerNode.
    // For writing testcase
    size_t inner_node_msg_count;

    // Maxium count of records in LeafNode
    // For writing testcase
    size_t leaf_node_record_count;

    /******************************
             Cache Parameters
    ******************************/

    // Maximum data size in cached nodes, in bytes
    size_t cache_limit;

    // When dirty nodes larger than this level, start writeback,
    // in percentage * 100
    unsigned int cache_dirty_high_watermark;

    // When dirty node is elder than this, start writeback,
    // in milliseconds
    unsigned int cache_dirty_expire;

    // How many dirty nodes're written back in a turn,
    // in percentage * 100
    unsigned int cache_writeback_ratio;

    // How often the flusher thread is waked up to check,
    // in milliseconds
    unsigned int cache_writeback_interval;

    // How many last used clean nodes're replaced out in a turn,
    // in percentage * 100
    unsigned int cache_evict_ratio;

    // When cache size grows larger than this level, start recycle some unused pages,
    // in percentage * 100
    unsigned int cache_evict_high_watermark;

    /********************************
            Layout Parameters
    ********************************/

    Compress compress;

    bool check_crc;
};

}

#endif
