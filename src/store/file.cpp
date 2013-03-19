// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "sys/sys.h"
#include "util/logger.h"
#include "cascadb/file.h"

using namespace std;
using namespace cascadb;

class BlockingAIOReqest {
public:
    BlockingAIOReqest() : mtx(), condvar(&mtx) {}

    Mutex       mtx;
    CondVar     condvar;
    AIOStatus   status;
};

static void blocking_aio_handler(void *context, AIOStatus status)
{
    BlockingAIOReqest* req = (BlockingAIOReqest*) context;
    // to prevent race condition when notify() is called before wait()
    req->mtx.lock();
    req->status = status;
    req->mtx.unlock();
    req->condvar.notify();
}

AIOStatus AIOFile::read(uint64_t offset, Slice buf)
{
    BlockingAIOReqest* req = new BlockingAIOReqest();
    req->mtx.lock();
    async_read(offset, buf, req, blocking_aio_handler);
    req->condvar.wait();
    AIOStatus status = req->status;
    req->mtx.unlock();
    delete req;
    return status;
}

AIOStatus AIOFile::write(uint64_t offset, Slice buf)
{
    BlockingAIOReqest* req = new BlockingAIOReqest();
    req->mtx.lock();
    async_write(offset, buf, req, blocking_aio_handler);
    req->condvar.wait();
    AIOStatus status = req->status;
    req->mtx.unlock();
    delete req;
    return status;
}