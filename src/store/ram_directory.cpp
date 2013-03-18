#include <vector>

#include "util/bits.h"
#include "util/logger.h"
#include "store/ram_directory.h"

using namespace std;
using namespace cascadb;

// Read data maybe not consistent if another thread is writing the same area concurrently
bool RAMFile::read(uint64_t offset, Slice data, size_t& res) {
    ScopedMutex lock(&mtx_);
    assert(refcnt_ > 0);
    uint64_t length = length_; 
    lock.unlock();

    if (offset >= length) {
        res = 0;
        return true;
    }

    size_t read = data.size();
    if (length < offset + data.size())
        read = length - offset;

    size_t idx = offset / RAMFILE_BLK_SIZE;
    size_t off = 0;
    size_t left = read;

    size_t len = RAMFILE_BLK_SIZE - offset % RAMFILE_BLK_SIZE;
    len = left < len ? left : len;
    memcpy((void*)data.data(), blks_[idx] + offset % RAMFILE_BLK_SIZE, len);
    left -= len;
    off += len;
    idx ++;

    while (left > 0) {
        len = left < RAMFILE_BLK_SIZE ? left : RAMFILE_BLK_SIZE;
        memcpy((void*)(data.data() + off), blks_[idx], len);
        left -= len;
        off += len;
        idx ++;
    }
    assert(left == 0);
    res = read;
    return true;
}

bool RAMFile::write(uint64_t offset, Slice data) {
    ScopedMutex lock(&mtx_);
    assert(refcnt_ > 0);
    uint64_t end = offset + data.size();
    if (end > total_) {
        // alloc buffer
        size_t more = (end - total_ + RAMFILE_BLK_SIZE - 1)/RAMFILE_BLK_SIZE;
        blks_.reserve(blks_.size() + more);
        for(size_t i = 0; i < more; i++ ) {
            char* blk = new char[RAMFILE_BLK_SIZE];
            if (blk) {
                blks_.push_back(blk);
                total_ += RAMFILE_BLK_SIZE;
            } else {
                LOG_ERROR("write RAMFile error: out of memory");
                return false;
            }
        }
    }
    lock.unlock();

    size_t idx = offset / RAMFILE_BLK_SIZE;
    size_t off = 0;
    size_t left = data.size();

    size_t len = RAMFILE_BLK_SIZE - offset % RAMFILE_BLK_SIZE;
    len = left < len ? left : len;
    memcpy(blks_[idx] + offset % RAMFILE_BLK_SIZE, data.data(), len);
    left -= len;
    off += len;
    idx ++;

    while (left > 0) {
        len = left < RAMFILE_BLK_SIZE ? left : RAMFILE_BLK_SIZE;
        memcpy(blks_[idx], data.data() + off, len);
        left -= len;
        off += len;
        idx ++;
    }
    assert(left == 0);

    lock.lock();
    if (length_ < end) {
        length_ = end;
    }

    return true;
}

void RAMFile::truncate(uint64_t offset)
{
    ScopedMutex lock(&mtx_);
    assert(refcnt_ > 0);

    size_t sz = (offset + (RAMFILE_BLK_SIZE - 1) / RAMFILE_BLK_SIZE);
    if (sz >= blks_.size())
        return;

    for (size_t i = sz; i < blks_.size(); i++ ) {
        delete[] blks_[i];
    }
    total_ = sz * RAMFILE_BLK_SIZE;
    length_ = offset;
    blks_.resize(sz);
}

class RAMSequenceFileReader : public SequenceFileReader {
public:
    RAMSequenceFileReader(RAMFile* file) : file_(file), offset_(0)
    {
        file_->inc_refcnt();
    }

    ~RAMSequenceFileReader()
    {
        close();
    }

    size_t read(Slice buf)
    {
        assert(file_);
        size_t res;
        file_->read(offset_, buf, res);
        offset_ += res;
        return res;
    }

    bool skip(size_t n)
    {
        assert(file_);
        offset_ += n;
        return true;
    }

    void close()
    {
        if (file_) {
            file_->dec_refcnt();
            file_ = NULL;
            offset_ = 0;
        }
    }

private:
    RAMFile *file_;

    size_t offset_;
};

class RAMSequenceFileWriter : public SequenceFileWriter {
public:
    RAMSequenceFileWriter(RAMFile* file) : file_(file), offset_(0)
    {
        file_->inc_refcnt();
    }

    ~RAMSequenceFileWriter()
    {
        close();
    }

    bool append(Slice buf)
    {
        assert(file_);
        bool res = file_->write(offset_, buf);
        offset_ += buf.size();
        return res;
    }

    bool flush()
    {
        return true;
    }

    void close()
    {
        if (file_) {
            file_->dec_refcnt();
            file_ = NULL;
            offset_ = 0;
        }
    }

private:
    RAMFile *file_;

    size_t offset_;
};

class RAMAIOFile : public AIOFile {
public:
    RAMAIOFile(RAMFile* file) : file_(file)
    {
        file_->inc_refcnt();
    }

    ~RAMAIOFile()
    {
        close();
    }

    AIOStatus read(uint64_t offset, Slice buf)
    {
        assert(file_);
        AIOStatus status;
        status.succ = file_->read(offset, buf, status.read);
        return status;
    }

    AIOStatus write(uint64_t offset, Slice buf)
    {
        assert(file_);
        AIOStatus status;
        status.succ = file_->write(offset, buf);
        return status;
    }

    void async_read(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        assert(file_);
        AIOStatus status;
        status.succ = file_->read(offset, buf, status.read);
        cb(context, status);
    }

    void async_write(uint64_t offset, Slice buf, void* context, aio_callback_t cb)
    {
        assert(file_);
        AIOStatus status;
        status.succ = file_->write(offset, buf);
        cb(context, status);
    }

    void truncate(uint64_t offset)
    {
        assert(file_);
        file_->truncate(offset);
    }

    void close()
    {
        if (file_) {
            file_->dec_refcnt();
            file_ = NULL;
        }
    }

private:
    RAMFile *file_;
};

RAMDirectory::~RAMDirectory()
{
    ScopedMutex lock(&mtx_);
    for(map<string, RAMFile*>::iterator it = files_.begin();
        it != files_.end(); it++ ) {
        it->second->dec_refcnt();
    }
    files_.clear();
}

bool RAMDirectory::file_exists(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    if (files_.find(filename) == files_.end()) {
        return false;
    }
    return true;
}

RAMFile* RAMDirectory::open_ramfile(const std::string& filename, bool create)
{
    RAMFile *file = NULL;

    map<string, RAMFile*>::iterator it = files_.find(filename);
    if (it == files_.end()) {
        if (create) {
            file = new RAMFile();
            file->inc_refcnt(); // hold a reference
            files_[filename] = file;
        }
    } else {
        file = it->second;
    }

    return file;
}

SequenceFileReader* RAMDirectory::open_sequence_file_reader(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    RAMFile *file = open_ramfile(filename, false);
    if (file) {
        return new RAMSequenceFileReader(file);
    }
    return NULL;
}

SequenceFileWriter* RAMDirectory::open_sequence_file_writer(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    RAMFile *file = open_ramfile(filename, true);
    assert(file);
    return new RAMSequenceFileWriter(file);
}

AIOFile* RAMDirectory::open_aio_file(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    RAMFile *file = open_ramfile(filename, true);
    assert(file);
    return new RAMAIOFile(file);
}

size_t RAMDirectory::file_length(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    RAMFile *file = open_ramfile(filename, false);
    assert(file);
    return file->length();
}

void RAMDirectory::rename_file(const std::string& from, const std::string& to)
{
    ScopedMutex lock(&mtx_);
    assert(files_.find(from) != files_.end() && files_.find(to) == files_.end());
    files_[to] = files_[from];
    files_.erase(from);
}

void RAMDirectory::delete_file(const std::string& filename)
{
    ScopedMutex lock(&mtx_);
    RAMFile *file = open_ramfile(filename, false);

    if (file) {
        file->dec_refcnt();
        files_.erase(filename);
    }
}

std::string RAMDirectory::to_string()
{
    string buf;
    buf += "RAMDirectory";
    return buf;
}

Directory* cascadb::create_ram_directory()
{
    return new RAMDirectory();
}