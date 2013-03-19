// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include <string.h>
#include <strings.h>

#include <sys/types.h>            
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <aio.h>
#include <errno.h>

#include "sys/sys.h"
#include "util/logger.h"

#include "posix_fs_directory.h"

using namespace std;
using namespace cascadb;

// A wrapper of POSIX file read API
class PosixSequenceFileReader : public SequenceFileReader {
public:
    PosixSequenceFileReader(const std::string& path) : path_(path), fd_(-1)
    {
    }

    ~PosixSequenceFileReader()
    {
        close();
    }

    bool open()
    {
        assert(fd_ < 0);
        fd_ = ::open(path_.c_str(), O_RDONLY);
        if (fd_ < 0) {
            LOG_ERROR("cannot open file " << path_ << ", error " << strerror(errno));
            return false;
        }
        return true;
    }

    size_t read(Slice buf)
    {
        ssize_t sz = ::read(fd_, (void*) buf.data(), buf.size());
        if (sz < 0) {
            LOG_ERROR("read file " << path_ << ", error " << strerror(errno));
            return 0;
        }
        return sz;
    }

    bool skip(size_t n)
    {
        if (::lseek(fd_, n, SEEK_CUR) < 0) {
            LOG_ERROR("skip file " << path_ << ", error " << strerror(errno));
            return false;
        }
        return true;
    }

    void close()
    {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    std::string path_;

    int fd_;
};

// A wrapper of POSIX file write API
class PosixSequenceFileWriter : public SequenceFileWriter {
public:
    PosixSequenceFileWriter(const std::string& path) : path_(path), fd_(-1), offset_(0)
    {
    }

    ~PosixSequenceFileWriter()
    {
        close();
    }

    bool open()
    {
        assert(fd_ < 0);
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            LOG_ERROR("cannot open file " << path_ << ", error " << strerror(errno));
            return false;
        }

        offset_ = ::lseek(fd_, 0, SEEK_CUR);
        return true;
    }

    bool append(Slice buf)
    {
        size_t sz;
        sz = ::write(fd_, buf.data(), buf.size());
        if (sz < 0) {
            LOG_ERROR("write file " << path_ << ", error " << strerror(errno));
            return false;
        }
        if (sz < buf.size()) {
            LOG_ERROR("storage is full " << path_);
            if (::ftruncate(fd_, offset_) < 0) {
                LOG_ERROR("ftruncate file error " << strerror(errno));
            }
            return false;
        }
        offset_ += sz;
        return true;
    }

    bool flush()
    {
        if (::fdatasync(fd_) < 0) {
            LOG_ERROR("flush file " << path_ << ", error " << strerror(errno));
            return false;
        }
        return true;
    }

    void close()
    {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
            offset_ = 0;
        }
    }

private:
    std::string path_;

    int fd_;

    size_t offset_;
};

class PosixAIORequest {
public:
    struct aiocb    aiocb;
    bool            read;
    size_t          size;
    void            *context;
    aio_callback_t  cb;
};

static void handle_posix_aio_complete(sigval_t sigval)
{
    AIOStatus status;
    PosixAIORequest *req = (PosixAIORequest*) sigval.sival_ptr;

    if (aio_error(&(req->aiocb)) == 0) {
        ssize_t ret = aio_return(&(req->aiocb));
        if (req->read) {
            if (ret < 0) {
                LOG_ERROR("posix aio read error: ");
                status.succ = false;
            } else {
                status.succ = true;
                status.read = ret;
            }
        } else {
            if (ret < 0 || (size_t)ret < req->size) {
                LOG_ERROR("posix aio write error: ");
                status.succ = false;
            } else {
                status.succ = true;
            }
        }
    } else {
        LOG_ERROR("posix aio handle io error");
        status.succ = false;
    }

    req->cb(req->context, status);
    delete req;
}

// A wrapper of POSIX AIO APIs
class PosixAIOFile : public AIOFile {
public:
    PosixAIOFile(const std::string& path) : path_(path), closed_(false), fd_(-1)
    {
    }

    ~PosixAIOFile()
    {
        close();
    }

    bool open()
    {
        fd_ = ::open(path_.c_str(), O_RDWR | O_DIRECT | O_CREAT, 0644);
        if (fd_ == -1) {
            LOG_ERROR("open file " << path_ << " error: " << strerror(errno));
            return false;
        }

        closed_ = false;
        return true;
    }

    void async_read(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        PosixAIORequest *req = new PosixAIORequest();
        prepare_aiocb(req, offset, buf);
        req->read = true;
        req->size = buf.size();
        req->context = context;
        req->cb = cb;

        while (true) {
            int ret = aio_read(&(req->aiocb));
            if (ret < 0) {
                if (errno == EAGAIN) {
                    LOG_INFO("posix aio_read busy, wait for a while");
                    cascadb::usleep(1000);
                    continue; // retry
                }
                LOG_ERROR("posix aio_read error: " << strerror(errno));
                AIOStatus status;
                status.succ = false;
                cb(context, status); // failed
            }
            return; // ok
        }
    }

    void async_write(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        PosixAIORequest *req = new PosixAIORequest();
        prepare_aiocb(req, offset, buf);
        req->read = false;
        req->size = buf.size();
        req->context = context;
        req->cb = cb;

        while (true) {
            int ret = aio_write(&(req->aiocb));
            if (ret < 0) {
                if (errno == EAGAIN) {
                    LOG_INFO("posix aio_write busy, wait for a while");
                    cascadb::usleep(1000);
                    continue; // retry
                }
                LOG_ERROR("posix aio_write error: " << strerror(errno));
                AIOStatus status;
                status.succ = false;
                cb(context, status); // failed
            }
            return; // ok
        }
    }

    void truncate(uint64_t offset)
    {
        if (::ftruncate(fd_, offset) < 0) {
            LOG_ERROR("ftruncate file error " << strerror(errno));
        }
        return;
    }

    void close()
    {
        if (!closed_) {
            closed_ = true;

            ::close(fd_);
            fd_ = -1;
        }
    }

protected:
    void prepare_aiocb(PosixAIORequest *req, uint64_t offset, Slice buf)
    {
        bzero(&(req->aiocb), sizeof(struct aiocb));
        req->aiocb.aio_fildes = fd_;
        req->aiocb.aio_buf = (char*) buf.data();
        req->aiocb.aio_nbytes = buf.size();
        req->aiocb.aio_offset = offset;

        req->aiocb.aio_sigevent.sigev_notify = SIGEV_THREAD;
        req->aiocb.aio_sigevent.sigev_notify_function = handle_posix_aio_complete;
        req->aiocb.aio_sigevent.sigev_notify_attributes = NULL;
        req->aiocb.aio_sigevent.sigev_value.sival_ptr = req;
    }

private:
    std::string     path_;
    bool            closed_;

    int             fd_;
};

PosixFSDirectory::~PosixFSDirectory()
{
}

bool PosixFSDirectory::file_exists(const std::string& filename)
{
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    if (stat(fullpath(filename).c_str(), &sb) == -1) {
        if (errno != ENOENT) {
            LOG_ERROR("stat file " << filename << " error " << strerror(errno));
        }
        return false;
    }
    return S_ISREG(sb.st_mode);
}

SequenceFileReader* PosixFSDirectory::open_sequence_file_reader(const std::string& filename)
{
    PosixSequenceFileReader* reader = new PosixSequenceFileReader(fullpath(filename));
    if (reader && reader->open()) {
        return reader;
    }
    delete reader;
    return NULL;
}

SequenceFileWriter* PosixFSDirectory::open_sequence_file_writer(const std::string& filename)
{
    PosixSequenceFileWriter* writer = new PosixSequenceFileWriter(fullpath(filename));
    if (writer && writer->open()) {
        return writer;
    }
    delete writer;
    return NULL;
}

AIOFile* PosixFSDirectory::open_aio_file(const std::string& filename)
{
    PosixAIOFile* file = new PosixAIOFile(fullpath(filename));
    if (file && file->open()) {
        return file;
    }
    delete file;
    return NULL;
}

size_t PosixFSDirectory::file_length(const std::string& filename)
{
    struct stat sb;
    memset(&sb, 0, sizeof(sb));
    if (stat(fullpath(filename).c_str(), &sb) == -1) {
        LOG_ERROR("stat file " << filename << " error " << strerror(errno));
        return 0;
    }
    return (size_t) sb.st_size;
}

const std::string PosixFSDirectory::fullpath(const std::string& filename)
{
    return dir_ + "/" + filename;
}

