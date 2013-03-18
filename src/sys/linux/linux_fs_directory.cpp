// std
#include <stdlib.h>
#include <stdexcept>

// posix
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <fcntl.h>
#include <unistd.h>

// linux
#ifdef HAS_LIBAIO
#include <libaio.h>
#endif

#include "sys/sys.h"
#include "util/logger.h"
#include "linux_fs_directory.h"

using namespace std;
using namespace cascadb;

#ifdef HAS_LIBAIO

#define MAX_AIO_EVENTS 128

static void* handle_io_complete(void *ptr);

enum AIOOP {
    AIORead,
    AIOWrite
};

class AIOTask {
public:
    AIOOP op;
    size_t size;
    void *context;
    aio_callback_t cb;
};

// A wrapper of Linxu libaio APIs
class LinuxAIOFile : public AIOFile {
public:
    LinuxAIOFile(const std::string& path) 
    : path_(path), closed_(false), fd_(-1), ctx_(0), thr_(NULL)
    {
    }

    ~LinuxAIOFile()
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

        int ret;
        if ((ret = io_setup(MAX_AIO_EVENTS, &ctx_)) < 0) {
            LOG_ERROR("linux aio io_setup error " << strerror(-1*ret));
            ::close(fd_);
            return false;
        }

        thr_ = new Thread(::handle_io_complete);
        thr_->start(this);

        closed_ = false;
        return true;
    }

    void handle_io_complete()
    {
        while (!closed_) {
            struct io_event events[MAX_AIO_EVENTS];
            memset(&events, 0, sizeof(struct io_event) * MAX_AIO_EVENTS);

            struct timespec timeout;
            timeout.tv_sec = 0;
            timeout.tv_nsec = 100000000; // 100ms

            int nevents;
            if ((nevents = ::io_getevents(ctx_, 1, MAX_AIO_EVENTS, events, &timeout)) < 0) {
                LOG_ERROR("linux aio io_getevents error " << strerror(-1*nevents));
                if (-1*nevents == EINTR) {
                    continue;
                } else {
                    break;
                }
            }

            for (int i = 0; i < nevents; i++) {
                AIOTask* task = (AIOTask*)events[i].data;
                assert(task);
                int res = events[i].res;
                switch(task->op) {
                case AIORead:
                {
                    AIOStatus status;
                    if (res < 0) {
                        LOG_ERROR("linux aio read error: " << strerror(-1*res));
                        status.succ = false;
                    } else {
                        status.succ = true;
                        status.read = res;
                    }
                    task->cb(task->context, status);
                    break;
                }
                case AIOWrite:
                {
                    AIOStatus status;
                    if (res < 0) {
                        LOG_ERROR("linux aio write error: " << strerror(-1*res));
                        status.succ = false;
                    } else if ((size_t)res < task->size) {
                        LOG_ERROR("linux aio write incomplete, should be " << task->size 
                            << " bytes, actually " << res << " bytes");
                        status.succ = false;
                    } else {
                        assert((size_t)res == task->size);
                        status.succ = true;
                    }
                    task->cb(task->context, status);
                    break;
                }
                }

                delete task;
            }
        }
    }

    void async_read(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        struct iocb iocb;
        struct iocb *iocbp = &iocb;

        LOG_TRACE("read " << buf.size() << " bytes from " << path_ << ":" << offset);

        // prepare
        AIOTask* task = new AIOTask();
        assert(task);
        task->op = AIORead;
        task->size = buf.size();
        task->context = context;
        task->cb = cb;
        io_prep_pread(iocbp, fd_, (void *)buf.data(), buf.size(), offset);
        iocbp->data = task;

        // submit
        if (!submit(1, &iocbp)) {
            AIOStatus status;
            status.succ = false;
            cb(context, status);
            delete task;
        }
    }

    void async_write(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        struct iocb iocb;
        struct iocb *iocbp = &iocb;

        LOG_TRACE("write " << buf.size() << " bytes out to " << path_ << ":" << offset);

        // prepare
        AIOTask* task = new AIOTask();
        assert(task);
        task->op = AIOWrite;
        task->size = buf.size();
        task->context = context;
        task->cb = cb;
        io_prep_pwrite(iocbp, fd_, (void *)buf.data(), buf.size(), offset);
        iocbp->data = task;

        // submit 
        if (!submit(1, &iocbp)) {
            AIOStatus status;
            status.succ = false;
            cb(context, status);
            delete task;
        }
    }

    void truncate(uint64_t offset)
    {
        if (::ftruncate(fd_, offset) < 0) {
            LOG_ERROR("ftruncate file error " << strerror(errno));
        }
    }

    void close()
    {
        if (!closed_) {
            closed_ = true;

            thr_->join();
            delete thr_;
            thr_ = NULL;

            int ret;
            if ((ret = io_destroy(ctx_)) < 0) {
                LOG_ERROR("linux aio io_destroy error " << strerror(-1*ret));
            }
            ctx_ = 0;
            ::close(fd_);
            fd_ = -1;
        } 
    }
protected:
    bool submit(int n, struct iocb **iocbpp) {
        // submit
        while (true) {
            int ret = io_submit(ctx_, n, iocbpp);
            if (ret < 0) {
                int errcode = -1 * ret;
                if (errcode == EAGAIN) {
                    LOG_INFO("linux aio io_submit busy, wait for a while");
                    cascadb::usleep(1000);
                    continue;
                }
                LOG_ERROR("linux aio io_submit error: " << strerror(errcode));
                return false;
            }
            assert(ret == n);
            return true;
        }
    }

private:
    std::string path_;

    bool closed_;

    int fd_;

    io_context_t ctx_; // AIO context

    Thread *thr_; // thread to handle io completion events
};

static void* handle_io_complete(void *ptr)
{
    LinuxAIOFile *aio_file = (LinuxAIOFile*) ptr;
    aio_file->handle_io_complete();
    return NULL;
}

#endif // LIBAIO

AIOFile* LinuxFSDirectory::open_aio_file(const std::string& filename)
{
#ifdef HAS_LIBAIO
    LinuxAIOFile* file = new LinuxAIOFile(fullpath(filename));
    if (file && file->open()) {
        return file; 
    }
    delete file;
    return NULL;
#else
    return PosixFSDirectory::open_aio_file(filename);
#endif
}

