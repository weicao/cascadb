// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_SERIALIZE_LAYOUT_H_
#define _CASCADB_SERIALIZE_LAYOUT_H_

#include <stddef.h>
#include <stdint.h>

#include <vector>
#include <set>
#include <map>

#include "cascadb/file.h"
#include "cascadb/options.h"
#include "sys/sys.h"
#include "util/callback.h"
#include "block.h"
#include "super_block.h"

namespace cascadb {

#define BLOCK_META_SIZE (64 + 32 + 32 + 16) / 8

// Metadata for block, stored inside index
struct BlockMeta {
    uint64_t    offset;             // start offset in file
    size_t      inflated_size;      // size before compression
    size_t      compressed_size;    // size after compression
    uint16_t    crc;                // crc of block data
};

// Storage layout, read blocks from file and write blocks into file

// TODO:
// 1. crc and more compression algorithm
// 2. fragmentation collection
// 3. recover from disaster

class Layout {
public:
    Layout(AIOFile* aio_file,
           size_t length,
           const Options& options);

    ~Layout();
   
    // Read and initialize SuperBlock if create is set to false
    // Otherwise, initialize and write SuperBlock out
    bool init(bool create = false);

    // blocking read
    Block* read(bid_t bid);

    // Initialize a read operation
    void async_read(bid_t bid, Block** block, Callback *cb);

    // Initiate a write operation
    void async_write(bid_t bid, Block* block, Callback *cb);
    
    // Delete block from index 
    void delete_block(bid_t bid);

    // Flush blocks and index out
    bool flush();

    // Flush meta data
    bool flush_meta();

    // Truncate unused space at file end,
    // invoked inside init/flush by default
    void truncate();

    // Construct a Block object
    Block* create(size_t limit);
    
    // Destrcut a Block object
    void destroy(Block* block);

protected:
    // read and deserialize superblock
    bool load_superblock();

    // read and deserialize the second superblock
    // superblock is double written to ensure correctness
    bool load_2nd_superblock();

    // Flush super block (double write)
    bool flush_superblock();

    // Read and deserialize the index block
    bool load_index();

    // Serialize index into the index block and write it out
    bool flush_index();

    // Deserialize superblock from buffer
    bool read_superblock(BlockReader& reader);

    // Serialize superblock into buffer
    bool write_superblock(BlockWriter& writer);

    // Deserialize index from buffer
    bool read_index(BlockReader& reader);

    // Calculate the size of buffer after index is serialized
    size_t get_index_size();

    // Serialize index into buffer
    bool write_index(BlockWriter& writer);

    // Deserialize block metadata from buffer
    bool read_block_meta(BlockMeta* meta, BlockReader& reader);

    // Serialize block metadata into buffer
    bool write_block_meta(BlockMeta* meta, BlockWriter& writer);

    // Context of async read operation
    struct AsyncReadReq {
        bid_t                   bid;
        Callback                *cb;
        Block                   **block;
        BlockMeta               meta;
        Slice                  buffer;
    };

    // called when AIOFile returns the result of async read
    void handle_async_read(AsyncReadReq *r, AIOStatus status);

    // Context of async write operation
    struct AsyncWriteReq {
        bid_t                   bid;
        Callback                *cb;
        BlockMeta               meta;
        Slice                  buffer;
    };

    // called when AIOFile returns the result of asyn write
    void handle_async_write(AsyncWriteReq *req, AIOStatus status);

    bool get_block_meta(bid_t bid, BlockMeta& meta);

    void set_block_meta(bid_t bid, const BlockMeta& meta);

    void del_block_meta(bid_t bid);

    bool read_block(const BlockMeta& meta, Block **block);

    // Read from offset and fill buffer
    bool read_data(uint64_t offset, Slice& buffer);

    // Write buffer into file offset
    bool write_data(uint64_t offset, Slice buffer);

    // Compress data in input_buffer and write into output_buffer,
    // input_buffer and output_buffer are all page aligned,
    // size before compression is set in input_size,
    // size after compression will be set to output_size
    bool compress_data(Slice input_buffer, size_t input_size,
                       Slice& output_buffer, size_t& output_size);

    // Uncompress data in input_buffer and write into output_buffer,
    // input_buffer and output_buffer are all page aligned,
    // size before uncompression is set in input_size,
    // size after uncompression is set in output_size
    bool uncompress_data(Slice input_buffer, size_t input_size,
                         Slice& output_buffer, size_t output_size);

    // get the position to write for given size
    uint64_t get_offset(size_t size);

    void print_index_info();

    void init_block_offset_index();

    void init_holes();

    void add_hole(uint64_t offset, size_t size);

    bool get_hole(size_t size, uint64_t& offset);

    Slice alloc_aligned_buffer(size_t size);

    void free_buffer(Slice buffer);

private:
    AIOFile                             *aio_file_;
    uint64_t                            length_; // file length
    Options                             options_;

    Mutex                               mtx_;

    // the offset to file end
    uint64_t                            offset_;

    SuperBlock                          *superblock_;

    Mutex                               block_index_mtx_;

    // Main index of BlockMeta for each blocks, indexed by offset
    typedef std::map<bid_t, BlockMeta*> BlockIndexType;
    BlockIndexType                      block_index_;

    // Auxiliary index of BlockMeta, sorted by offset in file.
    // including BlockMeta of block index
    typedef std::map<uint64_t, BlockMeta*> BlockOffsetIndexType;
    BlockOffsetIndexType                block_offset_index_;

    Mutex                               hole_list_mtx_;

    // A hole is generated when modify a block, the space taken up
    // before modification by the block becomes a hole
    struct Hole {
        uint64_t offset;
        uint64_t size;
    };

    // A chain of holes in file
    typedef std::deque<Hole>            HoleListType;
    HoleListType                        hole_list_;

    // the rest are statistics information
    size_t                              fly_writes_;    // todo atomic
    size_t                              fly_reads_;     // todo atomic
};

}

#endif