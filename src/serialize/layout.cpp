// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <malloc.h>

// to get inner/leaf node information
#include "tree/node.h"

#include "serialize/layout.h"
#include "util/logger.h"
#include "util/bits.h"

using namespace std;
using namespace cascadb;

static void aio_complete_handler(void *context, AIOStatus status)
{
    Callback *cb = (Callback *)context;
    assert(cb);
    cb->exec(status);
    delete cb;
}

Layout::Layout(AIOFile* aio_file, 
               size_t length,
               const Options& options)
: aio_file_(aio_file),
  length_(length),
  options_(options),
  offset_(0),
  superblock_(new SuperBlock),
  fly_writes_(0),
  fly_reads_(0)
{
}

Layout::~Layout()
{
    if (!flush()) {
        assert(false);
    }

    ScopedMutex block_index_lock(&block_index_mtx_);
    for (BlockIndexType::iterator it = block_index_.begin();
        it != block_index_.end(); it++ ) {
        delete it->second;
    }
    block_index_.clear();
    block_offset_index_.clear();
    block_index_lock.unlock();

    delete superblock_->index_block_meta;
    delete superblock_;
}

bool Layout::init(bool create)
{
    if (create) {
        // initialize and write super block out
        superblock_->index_block_meta = NULL;
        if (!flush_superblock()) {
            LOG_ERROR("flush superblock error during create");
            return false;
        }

        // superblock is double written
        ScopedMutex lock(&mtx_);
        offset_ = SUPER_BLOCK_SIZE * 2;
        length_ = offset_;
    } else {
        if (length_ < SUPER_BLOCK_SIZE * 2) {
            LOG_ERROR("data file is too short");
            return false;
        }
        if (!load_superblock()) {
            LOG_ERROR("read superblock error during init");
            return false;
        }
        if (superblock_->index_block_meta) {
            if (!load_index()) {
                LOG_ERROR("load index error");
                return false;
            }
        }
        init_block_offset_index();
        init_holes();
        print_index_info();

        ScopedMutex block_index_lock(&block_index_mtx_);
        LOG_INFO(block_index_.size() << " blocks found");
    }

    truncate();
    return true;
}

Block* Layout::read(bid_t bid, bool skeleton_only)
{
    BlockMeta meta;
    if (!get_block_meta(bid, meta)) {
        LOG_INFO("read block error, cannot find block bid " << hex << bid << dec);
        return NULL;
    }

    uint32_t read;
    if (skeleton_only) {
        read = meta.skeleton_size;
    } else {
        read = meta.total_size;
    }

    Block *block;
    if (!read_block(meta, 0, read, &block)) {
        LOG_ERROR("read block error, bid " << hex << bid << dec
            << ", offset " << meta.offset
            << ", size " << read);
        return NULL;
    }

    LOG_TRACE("read block ok,  bid " << hex << bid << dec 
        << ", offset " << meta.offset << ", size " << read);
    return block;
}

Block* Layout::read(bid_t bid, uint32_t offset, uint32_t size)
{
    BlockMeta meta;
    if (!get_block_meta(bid, meta)) {
        LOG_INFO("read block error, cannot find block bid " << hex << bid << dec);
        return NULL;
    }

    assert(offset <= meta.total_size);
    assert(offset + size <= meta.total_size);

    Block *block;
    if (!read_block(meta, offset, size, &block)) {
        LOG_ERROR("read block error, bid " << hex << bid << dec
            << ", offset " << (meta.offset + offset)
            << ", size " << size);
        return NULL;
    }

    LOG_TRACE("read block ok,  bid " << hex << bid << dec 
        << ", offset " << (meta.offset + offset)
        << ", size " << size)
    return block;
}

void Layout::async_read(bid_t bid, Block **block, Callback *cb)
{
    BlockMeta meta;
    if (!get_block_meta(bid, meta)) {
        LOG_INFO("Read Block failed, cannot find block bid " << hex << bid << dec);
        cb->exec(false);
        return;
    }

    Slice buffer = alloc_aligned_buffer(meta.total_size);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer fail, size " << meta.total_size);
        cb->exec(false);
        return;
    }

    AsyncReadReq *req = new AsyncReadReq();
    req->bid = bid;
    req->cb = cb;
    req->block = block;
    req->buffer = buffer;
    req->meta = meta;

    Callback *ncb = new Callback(this, &Layout::handle_async_read, req);

    ScopedMutex lock(&mtx_);
    fly_reads_ ++;
    lock.unlock();

    aio_file_->async_read(meta.offset, buffer, ncb, aio_complete_handler);
}

void Layout::handle_async_read(AsyncReadReq *req, AIOStatus status)
{
    ScopedMutex lock(&mtx_);
    fly_reads_ --;
    lock.unlock();

    if (status.succ) {
        LOG_TRACE("read block bid " << hex << req->bid << dec 
            << " at offset " << req->meta.offset << " ok");
        *(req->block) = new Block(req->buffer, 0, req->meta.total_size);
        req->cb->exec(true);
    } else {
        LOG_ERROR("read block bid " << hex << req->bid << dec << " error");
        free_buffer(req->buffer);
        req->cb->exec(false);
    }
    delete req->cb;
    delete req;
}

void Layout::async_write(bid_t bid, Block *block, uint32_t skeleton_size, Callback *cb)
{
    // Assumpt buffer inside block is aligned
    assert(block->capacity() == PAGE_ROUND_UP(block->size()));

    AsyncWriteReq *req = new AsyncWriteReq();
    req->bid = bid;
    req->cb = cb;
    req->meta.skeleton_size = skeleton_size;
    req->meta.total_size = block->size();
    req->buffer = block->buffer();
    req->meta.offset = get_offset(req->buffer.size());

    Callback *ncb = new Callback(this, &Layout::handle_async_write, req);

    ScopedMutex lock(&mtx_);
    fly_writes_ ++;
    lock.unlock();

    aio_file_->async_write(req->meta.offset, req->buffer, ncb, aio_complete_handler);
}

void Layout::handle_async_write(AsyncWriteReq *req, AIOStatus status)
{
    ScopedMutex lock(&mtx_);
    fly_writes_ --;
    lock.unlock();

    if (status.succ) {
        LOG_TRACE("write block bid " << hex << req->bid << dec
            << " at offset " << req->meta.offset << " ok");
        set_block_meta(req->bid, req->meta);
    } else {
        LOG_ERROR("write block " << req->bid << " error");
        add_hole(req->meta.offset, PAGE_ROUND_UP(req->meta.total_size));
    }

    req->cb->exec(status.succ);
    delete req->cb;

    delete req;
}

void Layout::delete_block(bid_t bid)
{
    BlockMeta meta;
    if (!get_block_meta(bid, meta)) {
        LOG_ERROR("Delete Block failed, cannot find block bid " << hex << bid << dec);
        return;
    }

    del_block_meta(bid);
}

bool Layout::flush()
{
    ScopedMutex lock(&mtx_);
    // waiting for blocks be written
    while (fly_writes_ > 0) {
        lock.unlock();
        cascadb::usleep(1000);
        lock.lock();
    }
    lock.unlock();

    if (!flush_meta()) return false;

    truncate();
    return true;
}

bool Layout::flush_meta()
{
    size_t fly_hole_size;

    ScopedMutex lock(&fly_hole_list_mtx_);
    fly_hole_size = fly_hole_list_.size();
    lock.unlock();

    if (!flush_index()) return false;
    if (!flush_superblock()) return false;

    // add fly holes to hole list
    flush_fly_holes(fly_hole_size);

    return true;
}

void Layout::truncate()
{
    ScopedMutex lock(&mtx_);
    if (offset_ < length_) {
        aio_file_->truncate(offset_);
        length_ = offset_;
    }
}

bool Layout::load_superblock()
{
    Slice buffer = alloc_aligned_buffer(SUPER_BLOCK_SIZE);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer error, size " << SUPER_BLOCK_SIZE);
        return false;
    }

    if (!read_data(0, buffer)) {
        LOG_ERROR("try to read 1st superblock error");
        return load_2nd_superblock();
    }

    LOG_TRACE("read 1st superblock ok");
    Block block(buffer, 0, SUPER_BLOCK_SIZE);
    BlockReader reader(&block);
    if (!read_superblock(reader)) {
        LOG_ERROR("1st superblock is invalid");
        free_buffer(buffer);
        return load_2nd_superblock();
    }

    LOG_TRACE("load 1st superblock ok");
    free_buffer(buffer);
    return true;
}

bool Layout::load_2nd_superblock()
{
    Slice buffer = alloc_aligned_buffer(SUPER_BLOCK_SIZE);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer error, size " << SUPER_BLOCK_SIZE);
        return false;
    }

    if (!read_data(SUPER_BLOCK_SIZE, buffer)) {
        LOG_ERROR("try to read 2nd superblock error");
        return false;
    }

    LOG_TRACE("read 2nd superblock ok");
    Block block(buffer, 0, SUPER_BLOCK_SIZE);
    BlockReader reader(&block);
    if (!read_superblock(reader)) {
        LOG_ERROR("2nd superblock is invalid");
        free_buffer(buffer);
        return false;
    }

    LOG_TRACE("load 2nd superblock ok");
    free_buffer(buffer);
    return true;
}

bool Layout::flush_superblock()
{
    Slice buffer = alloc_aligned_buffer(SUPER_BLOCK_SIZE);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer fail, size " << SUPER_BLOCK_SIZE);
        return false;
    }

    Block block(buffer, 0, 0);
    BlockWriter writer(&block);
    if (!write_superblock(writer)) {
        assert(false);
    }

    // double write to ensure superblock is correct

    if (!write_data(0, buffer)) {
        LOG_ERROR("flush 1st superblock error");
        free_buffer(buffer);
        return false;
    }

    LOG_TRACE("flush 1st superblock ok");

    if (!write_data(SUPER_BLOCK_SIZE, buffer)) {
        LOG_ERROR("flush 2nd superblock error");
        free_buffer(buffer);
        return false;
    }

    LOG_TRACE("flush 2nd superblock ok");
    free_buffer(buffer);
    return true;
}

bool Layout::load_index()
{
    BlockMeta *meta = superblock_->index_block_meta;
    assert(meta);

    LOG_TRACE("read index block from offset " << meta->offset);

    Block *block;
    if (!read_block(*meta, &block)) {
        LOG_ERROR("read index block error");
        return false;
    }

    BlockReader reader(block);
    if (!read_index(reader)) {
        LOG_ERROR("invalid index block");
        destroy(block);
        return false;
    }

    destroy(block);
    return true;
}

void Layout::flush_fly_holes(size_t fly_hole_size)
{
    size_t i;
    Hole flyhole;

    ScopedMutex fly_hole_list_lock(&fly_hole_list_mtx_);
    for (i = 0; i < fly_hole_size; i++) {
        flyhole = fly_hole_list_.front();
        add_hole(flyhole.offset, flyhole.size);
        fly_hole_list_.pop_front();
    }
}

bool Layout::flush_index()
{
    size_t size = get_index_size();

    Slice buffer = alloc_aligned_buffer(size);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer fail, size " << size);
        return false;
    }

    Block block(buffer, 0, 0);
    BlockWriter writer(&block);
    if (!write_index(writer)) {
        assert(false);
    }
    assert(block.size() == size);

    uint64_t offset = get_offset(buffer.size());
    if (!write_data(offset, buffer)) {
        LOG_ERROR("flush index block error");
        add_hole(offset, buffer.size());
        free_buffer(buffer);
        return false;
    }

    LOG_TRACE("flush index block ok");
    if (superblock_->index_block_meta) {
        add_fly_hole(superblock_->index_block_meta->offset, 
            PAGE_ROUND_UP(superblock_->index_block_meta->total_size));
    } else {
        superblock_->index_block_meta = new BlockMeta();
    }

    ScopedMutex block_index_lock(&block_index_mtx_);
    if (superblock_->index_block_meta) {
        block_offset_index_.erase(superblock_->index_block_meta->offset);
    }
    block_offset_index_[offset] = superblock_->index_block_meta;
    block_index_lock.unlock();

    superblock_->index_block_meta->offset = offset;
    superblock_->index_block_meta->total_size = size;

    free_buffer(buffer);
	
    return true;
}

bool Layout::read_superblock(BlockReader& reader)
{
    if (!reader.readUInt64(&(superblock_->magic_number))) return false;
    if (!reader.readUInt8(&(superblock_->major_version))) return false;
    if (!reader.readUInt8(&(superblock_->minor_version))) return false;

    bool has_index_block_meta;
    if (!reader.readBool(&has_index_block_meta)) return false;
    if (has_index_block_meta) {
        superblock_->index_block_meta = new BlockMeta();
        if (!read_block_meta(superblock_->index_block_meta, reader)) return false;
    }

    if (!reader.readUInt16(&(superblock_->crc))) return false;
    return true;
}

bool Layout::write_superblock(BlockWriter& writer)
{
    if (!writer.writeUInt64(superblock_->magic_number)) return false;
    if (!writer.writeUInt8(superblock_->major_version)) return false;
    if (!writer.writeUInt8(superblock_->minor_version)) return false;

    if (superblock_->index_block_meta) {
        if (!writer.writeBool(true)) return false;
        if (!write_block_meta(superblock_->index_block_meta, writer)) return false;
    } else {
        if (!writer.writeBool(false)) return false;
    }

    if (!writer.writeUInt16(superblock_->crc)) return false;
    return true;
}

bool Layout::read_index(BlockReader& reader)
{
    ScopedMutex block_index_lock(&block_index_mtx_);
    assert(block_index_.size() == 0);

    uint32_t n;
    if (!reader.readUInt32(&n)) return false;
    for (uint32_t i = 0; i < n; i++ ) {
        bid_t bid;
        BlockMeta* meta = new BlockMeta();
        if (reader.readUInt64(&bid) && read_block_meta(meta, reader)) {
            block_index_[bid] = meta;
            continue;
        }
        delete meta;
        return false;
    }
    return true;
}

size_t Layout::get_index_size()
{
    ScopedMutex block_index_lock(&block_index_mtx_);
    return 4 + block_index_.size() *  // count + block meta
        (8 + BLOCK_META_SIZE) ;   // key + value
}

bool Layout::write_index(BlockWriter& writer)
{
    ScopedMutex block_index_lock(&block_index_mtx_);
    if (!writer.writeUInt32(block_index_.size())) return false;
    for(map<bid_t, BlockMeta*>::iterator it = block_index_.begin();
        it != block_index_.end(); it++ ) {
        if (!writer.writeUInt64(it->first)) return false;
        if (!write_block_meta(it->second, writer)) return false;
    }
    return true;
}

bool Layout::read_block_meta(BlockMeta* meta, BlockReader& reader)
{
    if (!reader.readUInt64(&(meta->offset))) return false;
    if (!reader.readUInt32(&(meta->skeleton_size))) return false;
    if (!reader.readUInt32(&(meta->total_size))) return false;
    if (!reader.readUInt16(&(meta->crc))) return false;
    return true;
}

bool Layout::write_block_meta(BlockMeta* meta, BlockWriter& writer)
{
    if (!writer.writeUInt64(meta->offset)) return false;
    if (!writer.writeUInt32(meta->skeleton_size)) return false;
    if (!writer.writeUInt32(meta->total_size)) return false;
    if (!writer.writeUInt16(meta->crc)) return false;
    return true;
}

bool Layout::get_block_meta(bid_t bid, BlockMeta& meta)
{
    ScopedMutex lock(&block_index_mtx_);
    BlockIndexType::iterator it = block_index_.find(bid);
    if (it == block_index_.end()) {
        return false;
    }
    meta = *(it->second);
    return true;
}

void Layout::set_block_meta(bid_t bid, const BlockMeta& meta)
{
    BlockMeta *p;

    ScopedMutex lock(&block_index_mtx_);
    BlockIndexType::iterator it = block_index_.find(bid);
    if (it == block_index_.end()) {
        p = new BlockMeta();
        block_index_[bid] = p;
    } else {
        p = it->second;
        block_offset_index_.erase(p->offset);
        add_fly_hole(p->offset, PAGE_ROUND_UP(p->total_size));
    }

    *p = meta;
    block_offset_index_[meta.offset] = p;
    lock.unlock();
}

void Layout::del_block_meta(bid_t bid)
{
    ScopedMutex lock(&block_index_mtx_);

    BlockIndexType::iterator it = block_index_.find(bid);
    if (it != block_index_.end()) {
        BlockMeta *p = it->second;
        uint64_t offset;
        size_t size;

        block_index_.erase(it);
        offset = p->offset;
        size = PAGE_ROUND_UP(p->total_size);
        block_offset_index_.erase(p->offset);

        delete p;

        lock.unlock();
        add_fly_hole(offset, size);
    }
}

bool Layout::read_block(const BlockMeta& meta, Block **block)
{
    return read_block(meta, 0, meta.total_size, block);
}

bool Layout::read_block(const BlockMeta& meta, uint32_t offset, uint32_t size, Block **block)
{
    uint32_t offset1 = PAGE_ROUND_DOWN(offset);
    uint32_t size1 = offset - offset1 + size;

    Slice buffer = alloc_aligned_buffer(size1);
    if (!buffer.size()) {
        LOG_ERROR("alloc_aligned_buffer error, size " << size1);
        return false;
    }

    if (!read_data(meta.offset + offset1, buffer)) {
        free_buffer(buffer);
        return false;
    }

    *block = new Block(buffer, offset - offset1, size);
    return true;
}

bool Layout::read_data(uint64_t offset, Slice& buffer)
{
    LOG_TRACE("read file offset " << offset << ", buffer size " << buffer.size());

    ScopedMutex lock(&mtx_);
    fly_reads_ ++;
    lock.unlock();

    AIOStatus status = aio_file_->read(offset, buffer);

    lock.lock();
    fly_reads_ --;
    lock.unlock();

    if (!status.succ) {
        LOG_ERROR("read file offset " << offset << ", size " << buffer.size() << " error");
        return false;
    }
    return true;
}

bool Layout::write_data(uint64_t offset, Slice buffer)
{
    LOG_TRACE("write file offset " << offset << ", size " << buffer.size());

    ScopedMutex lock(&mtx_);
    fly_writes_ ++;
    lock.unlock();

    AIOStatus status = aio_file_->write(offset, buffer);

    lock.lock();
    fly_writes_ --;
    lock.unlock();
    
    if (!status.succ) {
        LOG_ERROR("write file offset " << offset << ", size " << buffer.size() << " error");
        return false;
    }

    return true;
}

uint64_t Layout::get_offset(size_t size)
{
    uint64_t offset;
    if (get_hole(size, offset)) {
        return offset;
    }

    ScopedMutex lock(&mtx_);
    offset = offset_;
    offset_ += size;

    // offset_ is possible to decrease when holes 
    // at the end of file is collected
    if(offset_ > length_) {
        length_ = offset_;
    }

    return offset;
}

void Layout::print_index_info()
{
    size_t inner_cnt = 0;
    size_t inner_total_size = 0;
    size_t leaf_cnt = 0;
    size_t leaf_total_size = 0;

    ScopedMutex block_index_lock(&block_index_mtx_);

    for (BlockIndexType::iterator it = block_index_.begin();
        it != block_index_.end(); it++ ) {
        if (IS_LEAF(it->first)) {
            leaf_cnt ++;
            leaf_total_size += it->second->total_size;
        } else {
            inner_cnt ++;
            inner_total_size += it->second->total_size;
        }
     }

     LOG_INFO("inner nodes count " << inner_cnt
        << ", total size " << inner_total_size << endl
        << "leaf node count " << leaf_cnt
        << ", total size " << leaf_total_size);
}

void Layout::init_block_offset_index()
{
    ScopedMutex block_index_lock(&block_index_mtx_);

    for (BlockIndexType::iterator it = block_index_.begin(); 
        it != block_index_.end(); it++ ) {
        block_offset_index_[it->second->offset] = it->second;
    }

    if (superblock_->index_block_meta) {
        block_offset_index_[superblock_->index_block_meta->offset] =
            superblock_->index_block_meta;
    }
}

void Layout::init_holes()
{
    ScopedMutex block_index_lock(&block_index_mtx_);

    BlockOffsetIndexType::iterator it = block_offset_index_.begin();
    BlockOffsetIndexType::iterator prev;
    for (; it != block_offset_index_.end(); it++ ) {
        uint64_t last;
        if (it == block_offset_index_.begin()) {
            last = SUPER_BLOCK_SIZE * 2;
        } else {
            last = prev->second->offset + PAGE_ROUND_UP(prev->second->total_size);
        }

        if (it->second->offset > last) {
            add_hole(last, it->second->offset - last);
        }
        prev = it;
    }

    ScopedMutex lock(&mtx_);

    // set file offset
    if (block_offset_index_.size())  {
        offset_ = block_offset_index_.rbegin()->second->offset +
            PAGE_ROUND_UP(block_offset_index_.rbegin()->second->total_size);
    } else {
        offset_ = SUPER_BLOCK_SIZE * 2;
    }
}

void Layout::add_hole(uint64_t offset, size_t size)
{
    Hole hole;
    hole.offset = offset;
    hole.size = size;

    ScopedMutex lock(&mtx_);
    if (offset + size == offset_) {
        offset_ = offset;
        return;
    }
    lock.unlock();

    ScopedMutex hole_list_lock(&hole_list_mtx_);

    HoleListType::iterator first = hole_list_.begin();
    HoleListType::iterator last = hole_list_.end();
    HoleListType::iterator prev = first;
    HoleListType::iterator it;

    if (first == last) {
        hole_list_.push_back(hole);
        return;
    }

    // binary search
    iterator_traits<HoleListType::iterator>::difference_type count, step;
    count = distance(first,last);
    while (count > 0) {
        it = first; 
        step = count/2; 
        advance(it, step);

        if (it->offset < hole.offset) {
            prev = it;
            first = ++ it;
            count -= step + 1;
        } else {
            count = step;
        }
    }

    assert(prev != hole_list_.end());

    if (prev == hole_list_.begin() && prev->offset > hole.offset) {
        assert(hole.offset + hole.size <= prev->offset);

        if (hole.offset + hole.size == prev->offset) {
            // merge holes
            prev->offset = hole.offset;
            prev->size += hole.size;
        } else {
            hole_list_.push_front(hole);
        }
    } else {
        assert(prev->offset < hole.offset);
        assert(prev->offset + prev->size <= hole.offset);

        HoleListType::iterator next = prev;
        advance(next, 1);

        if (prev->offset + prev->size == hole.offset) {
            // merge holes
            prev->size += hole.size;
        } else {
            prev = hole_list_.insert(next, hole);
        }

        if (next != hole_list_.end() && prev->offset + prev->size == next->offset) {
            // merge holes
            prev->size += next->size;
            hole_list_.erase(next);
        }
    }
}

void Layout::add_fly_hole(uint64_t offset, size_t size)
{
    Hole hole;
    hole.offset = offset;
    hole.size = size;

    ScopedMutex fly_hole_list_lock(&fly_hole_list_mtx_);
    fly_hole_list_.push_back(hole);
}

bool Layout::get_hole(size_t size, uint64_t& offset)
{
    ScopedMutex hole_list_lock(&hole_list_mtx_);

    HoleListType::iterator it = hole_list_.begin();
    for (; it != hole_list_.end(); it ++ ) {
        if (it->size > size) {
            offset = it->offset;

            it->offset += size;
            it->size -= size;
            return true;
        } else if (it->size == size) {
            offset = it->offset;

            hole_list_.erase(it);
            return true;
        }
    }
    return false;
}

Slice Layout::alloc_aligned_buffer(size_t size)
{
    assert(size);
    size_t rounded_size = PAGE_ROUND_UP(size);

    void *buf;
    if (posix_memalign(&buf, PAGE_SIZE, rounded_size)) {
        assert(false);
    }

    if (buf) {
        assert( ((size_t)buf & (PAGE_SIZE-1)) == 0);
        return Slice((char *)buf, rounded_size);
    } else {
        return Slice(); // empty
    }
}

void Layout::free_buffer(Slice buffer)
{
    if (buffer.size()) {
        free((char* )buffer.data()); // non-empty
    }
}

Block* Layout::create(size_t size)
{
    Slice buffer = alloc_aligned_buffer(size);
    if (buffer.size()) {
        return new Block(buffer, 0, 0);
    } else {
        return NULL;
    }
}

void Layout::destroy(Block* block)
{
    assert(block);
    free_buffer(block->buffer());
    delete block;
}
