// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_FILE_H_
#define CASCADB_FILE_H_

#include "slice.h"

namespace cascadb {

class SequenceFileReader {
public:
    SequenceFileReader() {}
    virtual ~SequenceFileReader() {}

    // read a number of bytes from the end of file into buffer,
    // The function will block until data is ready,
    // Return number of bytes read.
    virtual size_t read(Slice buf) = 0;

    // skip a number of bytes
    virtual bool skip(size_t n) = 0;

    virtual void close() = 0;

private:
    SequenceFileReader(const SequenceFileReader&);
    void operator=(const SequenceFileReader&);   
};

class SequenceFileWriter {
public:
    SequenceFileWriter() {}
    virtual ~SequenceFileWriter() {}

    // append buffer to the end of file,
    // The function will block until data is ready,
    // Return true if written successfully
    virtual bool append(Slice buf) = 0;
    virtual bool flush() = 0;
    virtual void close() = 0;

private:
    SequenceFileWriter(const SequenceFileWriter&);
    void operator=(const SequenceFileWriter&);   
};

struct AIOStatus {
    AIOStatus(): succ(false), read(0) {}

    bool    succ;       // whether AIO read/write completes successfully
    size_t  read;       // number of bytes read in AIO read
};

typedef void (*aio_callback_t)(void* context, AIOStatus status);

class AIOFile {
public:
    AIOFile() {}
    virtual ~AIOFile() {}

    // blocking wrapper to async read
    virtual AIOStatus read(uint64_t offset, Slice buf);

    // blocking wrapper to async write
    virtual AIOStatus write(uint64_t offset, Slice buf);

    // prepare to read a number of bytes at specified offset into buffer,
    // Callback will be invoked after the write operation completes,
    // The context parameter will be passed back into callback
    virtual void async_read(uint64_t offset, Slice buf, void* context, aio_callback_t cb) = 0;

    // prepare to write bytes in buffer to file at specified offset,
    // Callback will be invoked after the write operation completes,
    // The context parameter will be passed back into callback
    virtual void async_write(uint64_t offset, Slice buf, void* context, aio_callback_t cb) = 0;

    virtual void truncate(uint64_t offset) {}

    virtual void close() = 0;

private:
    AIOFile(const AIOFile&);
    void operator=(const AIOFile&);
};

}

#endif