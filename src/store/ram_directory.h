// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_STORE_RAM_DIRECTORY_H_
#define _CASCADB_STORE_RAM_DIRECTORY_H_

#include <map>
#include <vector>

#include "cascadb/directory.h"
#include "sys/sys.h"

namespace cascadb {

#define RAMFILE_BLK_SIZE 4096

class RAMFile {
public:
    RAMFile() : refcnt_(0), total_(0), length_(0) {}

    void inc_refcnt() {
        ScopedMutex lock(&mtx_);
        refcnt_ ++;
    }

    void dec_refcnt() {
        ScopedMutex lock(&mtx_);
        refcnt_ --;
        if (refcnt_ == 0) {
            lock.unlock();
            delete this; // fixme: possible to have race condition here
        }
    }

    int get_refcnt() {
        ScopedMutex lock(&mtx_);
        return refcnt_;
    }

    // Read data maybe not consistent if another thread is writing the same area concurrently
    bool read(uint64_t offset, Slice data, size_t& res);

    bool write(uint64_t offset, Slice data);

    void truncate(uint64_t offset);

    uint64_t length() {
        ScopedMutex lock(&mtx_);
        return length_;
    }

private:
    ~RAMFile() {
        assert(refcnt_ == 0);
        for (std::vector<char*>::iterator it = blks_.begin();
            it != blks_.end(); it ++ ) {
            delete [] (*it);
        }
    }

    Mutex               mtx_;
    size_t              refcnt_;

    std::vector<char*>  blks_;      // memory blocks allocated
    uint64_t            total_;     // total memory in allocated in blks_
    uint64_t            length_;    // length of RAMFile
};

class RAMDirectory : public Directory {
public:
    RAMDirectory()
    {
    }

    ~RAMDirectory();

    bool file_exists(const std::string& filename);

    SequenceFileReader* open_sequence_file_reader(const std::string& filename);

    SequenceFileWriter* open_sequence_file_writer(const std::string& filename);

    AIOFile* open_aio_file(const std::string& filename);

    size_t file_length(const std::string& filename);

    void rename_file(const std::string& from, const std::string& to);

    void delete_file(const std::string& filename);

    std::string to_string();

protected:
    // create if RAMFile not exist and the create flag is set
    RAMFile* open_ramfile(const std::string& filename, bool create = false);

private:
    Mutex                           mtx_;
    std::map<std::string, RAMFile*> files_;
};

}

#endif
