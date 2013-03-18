#include "util/logger.h"
#include "store/ram_directory.h"
#include "sys/linux/linux_fs_directory.h"
#include "db_impl.h"

using namespace std;
using namespace cascadb;
    
#define DAT_FILE_SUFFIX "cdb"

DBImpl::~DBImpl()
{
    delete tree_;
    delete node_store_;
    delete cache_;
    delete layout_;
    delete file_;
}

bool DBImpl::init()
{
    Directory *dir = options_.dir;
    if (!dir) {
        LOG_ERROR("dir must be set in options");
        return false;
    }

    string filename = name_ + "." + DAT_FILE_SUFFIX;
    size_t length = 0;
    bool create = true;
    if (dir->file_exists(filename)) {
        length = dir->file_length(filename);
        if (length > 0) {
            create = false;
        }
    }
    LOG_INFO("init db , data file length " << length << ", create " << create);

    file_ = dir->open_aio_file(filename);
    layout_ = new Layout(file_, length, options_);
    if (!layout_->init(create)) {
        LOG_ERROR("init layout error");
        return false;
    }

    cache_ = new Cache(options_);
    if (!cache_->init()) {
        LOG_ERROR("init cache error");
        return false;
    }

    node_store_ = new CachedNodeStore(cache_, name_, layout_);
    
    tree_ = new Tree(name_, options_, node_store_);
    if (!tree_->init()) {
        LOG_ERROR("tree init error");
        return false;
    }

    return true;
}

bool DBImpl::put(Slice key, Slice value)
{
    return tree_->put(key, value);
}

bool DBImpl::del(Slice key)
{
    return tree_->del(key);
}

bool DBImpl::get(Slice key, Slice& value)
{
    return tree_->get(key, value);
}

void DBImpl::flush()
{
    cache_->flush_table(name_);
}

void DBImpl::debug_print(std::ostream& out)
{
    cache_->debug_print(out);
}

DB* cascadb::DB::open(const std::string& name, const Options& options)
{
    DBImpl* db = new DBImpl(name, options);
    if (!db->init()) {
        delete db;
        return NULL;
    }
    return db;
}