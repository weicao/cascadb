#include <map>
#include <gtest/gtest.h>
#include <stdlib.h>

#include "sys/sys.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"

using namespace std;
using namespace cascadb;

class LayoutTest : public testing::Test {
public:
    LayoutTest() {
        dir = new RAMDirectory();
        file = NULL;
        layout = NULL;
        min_page_size = 1;
        max_page_size = 64 * 1024;

        srand(0);
    }

    ~LayoutTest() {
        delete dir;
    }

    virtual void SetUp()
    {
        dir->delete_file("layout_test");
        file = dir->open_aio_file("layout_test");
    }

    virtual void TearDown()
    {
        delete file;
        dir->delete_file("layout_test");
    }

    uint64_t GetLength() {
        return dir->file_length("layout_test");
    }

    void OpenLayout(const Options& opts, bool create) {
        size_t length;
        if (create) {
            length = 0;
        } else {
            length = dir->file_length("layout_test");
        }
        layout = new Layout(file, length, opts);
        ASSERT_TRUE(layout->init(create));
    }

    void CloseLayout() {
        ASSERT_TRUE(layout->flush());
        delete layout;
    }

    void Write() {
        results.clear();
        for (int i = 0; i < 1000; i++ ) {
            size_t size = min_page_size + rand() % (max_page_size - min_page_size);
            write_bufs[i] = layout->create(size);
            BlockWriter writer(write_bufs[i]);
            for (size_t j = 0; j < size; j++ ) {
                writer.writeUInt8(i&0xff);
            }
            Callback *cb = new Callback(this, &LayoutTest::callback, (bid_t)i);
            layout->async_write(i, write_bufs[i], size, cb);
        }

        while(results.size() != 1000) cascadb::usleep(10000); // 10ms
        for (int i = 0; i < 1000; i++ ) {
            ASSERT_TRUE(results[i]);
        }
    }

    void ClearWriteBufs() {
        for (map<bid_t, Block*>::iterator it = write_bufs.begin();
            it != write_bufs.end(); it++ ) {
            layout->destroy(it->second);
        }
        write_bufs.clear();
    }

    void AsyncRead() {
        results.clear();
        Block *read_bufs[1000];
        for (int i = 0; i < 1000; i++ ) {
            Callback *cb = new Callback(this, &LayoutTest::callback, (bid_t)i);
            layout->async_read(i, &(read_bufs[i]), cb);
        }

        while(results.size() != 1000) cascadb::usleep(10000); // 10ms
        for (int i = 0; i < 1000; i++ ) {
            ASSERT_TRUE(results[i]);
            if (results[i]) {
                ASSERT_EQ(write_bufs[i]->size(), read_bufs[i]->size());
                ASSERT_EQ(0, memcmp(write_bufs[i]->start(), read_bufs[i]->start(), write_bufs[i]->size()));
            }
            layout->destroy(read_bufs[i]);
        }
    } 

    void BlockingRead() {
        for (int i = 0; i < 1000; i++ ) {
            Block *read_buf = layout->read(i, false);
            ASSERT_TRUE(read_buf != NULL);
            ASSERT_EQ(write_bufs[i]->size(), read_buf->size());
            ASSERT_EQ(0, memcmp(write_bufs[i]->start(), read_buf->start(), write_bufs[i]->size()));
            layout->destroy(read_buf);
        }
    }

    void callback(bid_t bid, bool is_succ)
    {
        ScopedMutex lock(&mtx);
        results[bid] = is_succ;
    }

    Directory                   *dir;
    AIOFile                     *file;
    Layout                      *layout;

    size_t                      min_page_size;
    size_t                      max_page_size;
    map<bid_t, Block*>          write_bufs;

    Mutex                       mtx;
    map<bid_t, bool>            results;
};


TEST_F(LayoutTest, async_read)
{
    Options opts;

    OpenLayout(opts, true);
    Write();
    CloseLayout();

    OpenLayout(opts, false);
    AsyncRead();
    CloseLayout();

    ClearWriteBufs();
}

TEST_F(LayoutTest, blocking_read)
{
    Options opts;

    OpenLayout(opts, true);
    Write();
    CloseLayout();
    
    OpenLayout(opts, false);
    BlockingRead();
    CloseLayout();

    ClearWriteBufs();
}

TEST_F(LayoutTest, async_read_compress)
{
    Options opts;
    opts.compress = kSnappyCompress;

    OpenLayout(opts, true);
    Write();
    CloseLayout();

    OpenLayout(opts, false);
    AsyncRead();
    CloseLayout();

    ClearWriteBufs();
}

TEST_F(LayoutTest, blocking_read_compress)
{
    Options opts;
    opts.compress = kSnappyCompress;

    OpenLayout(opts, true);
    Write();
    CloseLayout();
    
    OpenLayout(opts, false);
    BlockingRead();
    CloseLayout();

    ClearWriteBufs();
}

TEST_F(LayoutTest, update)
{
    Options opts;

    OpenLayout(opts, true);
    Write();
    CloseLayout();

    OpenLayout(opts, false);
    AsyncRead();
    CloseLayout();

    ClearWriteBufs();

    uint64_t len1 = GetLength();

    OpenLayout(opts, false);
    Write();    // update all records
    CloseLayout();

    OpenLayout(opts, false);
    AsyncRead();
    CloseLayout();

    ClearWriteBufs();

    uint64_t len2 = GetLength();
    EXPECT_TRUE(len2 > len1 * 0.9 && len2 < len1 * 1.1); // fragment collection should works
}
