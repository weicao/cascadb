#include <iostream>

#include <gtest/gtest.h>

#include "cascadb/db.h"

using namespace std;
using namespace cascadb;

TEST(DB, put) {
    Options opts;
    opts.dir = create_ram_directory();
    opts.comparator = new LexicalComparator();

    DB *db = DB::open("test_db", opts);
    ASSERT_TRUE(db != NULL);

    ASSERT_TRUE(db->put("key1", "value1"));
    ASSERT_TRUE(db->put("key2", "value2"));
    ASSERT_TRUE(db->put("key3", "value3"));

    string value;
    ASSERT_TRUE(db->get("key1", value));
    ASSERT_EQ("value1", value);

    ASSERT_TRUE(db->get("key2", value));
    ASSERT_EQ("value2", value);

    ASSERT_TRUE(db->get("key3", value));
    ASSERT_EQ("value3", value);

    delete db;
    delete opts.dir;
    delete opts.comparator;
}

TEST(DB, del) {
    Options opts;
    opts.dir = create_ram_directory();
    opts.comparator = new LexicalComparator();

    DB *db = DB::open("test_db", opts);
    ASSERT_TRUE(db != NULL);

    ASSERT_TRUE(db->put("key1", "value1"));
    ASSERT_TRUE(db->put("key2", "value2"));
    ASSERT_TRUE(db->put("key3", "value3"));

    ASSERT_TRUE(db->del("key2"));

    string value;
    ASSERT_TRUE(db->get("key1", value));
    ASSERT_EQ("value1", value);

    ASSERT_FALSE(db->get("key2", value));

    ASSERT_TRUE(db->get("key3", value));
    ASSERT_EQ("value3", value);

    delete db;
    delete opts.dir;
    delete opts.comparator;
}

TEST(DB, batch_write) {
    Options opts;
    opts.dir = create_ram_directory();
    opts.comparator = new NumericComparator<uint64_t>();
    opts.inner_node_page_size = 4 * 1024;
    opts.inner_node_children_number = 64;
    opts.leaf_node_page_size = 4 * 1024;
    opts.leaf_node_bucket_size = 512;
    opts.cache_limit = 32 * 1024;
    opts.compress = kNoCompress;

    DB *db = DB::open("test_db", opts);
    ASSERT_TRUE(db != NULL);

    for (uint64_t i = 0; i < 100000; i++ ) {
        char buf[16] = {0};
        sprintf(buf, "%ld", i);
        Slice key = Slice((char*)&i, sizeof(uint64_t));
        Slice value = Slice(buf, strlen(buf));
        ASSERT_TRUE(db->put(key, value)) << "put key " << i << " error";
        if (i % 10000 == 0) {
            cout << "write " << i << " records" << endl;
        }
    }

    db->flush();

    for (uint64_t i = 0; i < 100000; i++ ) {
        Slice key = Slice((char*)&i, sizeof(uint64_t));
        Slice value;
        ASSERT_TRUE(db->get(key, value)) << "get key " << i << " error";

        char buf[16] = {0};
        sprintf(buf, "%ld", i);
        ASSERT_EQ(value.size(), strlen(buf)) << "get key " << i << " value size unequal" ;
        ASSERT_TRUE(strncmp(buf, value.data(), value.size()) == 0) << "get key " << i << " value data unequal";
        value.destroy();

        if (i % 10000 == 0) {
            cout << "read " << i << " records" << endl;
        }
    }

    delete db;
    delete opts.dir;
    delete opts.comparator;
}

TEST(DB, batch_delete) {
    Options opts;
    opts.dir = create_ram_directory();
    opts.comparator = new NumericComparator<uint64_t>();
    opts.inner_node_page_size = 4 * 1024;
    opts.inner_node_children_number = 64;
    opts.leaf_node_page_size = 4 * 1024;
    opts.leaf_node_bucket_size = 512;
    opts.cache_limit = 32 * 1024;
    opts.compress = kNoCompress;

    DB *db = DB::open("test_db", opts);
    ASSERT_TRUE(db != NULL);

    for (uint64_t i = 0; i < 100000; i++ ) {
        char buf[16] = {0};
        sprintf(buf, "%ld", i);
        Slice key = Slice((char*)&i, sizeof(uint64_t));
        Slice value = Slice(buf, strlen(buf));
        ASSERT_TRUE(db->put(key, value)) << "put key " << i << " error";
        if (i % 10000 == 0) {
            cout << "write " << i << " records" << endl;
        }
    }

    db->flush();

    for (uint64_t i = 0; i < 100000; i++ ) {
        Slice key = Slice((char*)&i, sizeof(uint64_t));
        ASSERT_TRUE(db->del(key)) << "del key " << i << " error";
        if (i % 10000 == 0) {
            cout << "del " << i << " records" << endl;
        }
    }

    db->flush();

    for (uint64_t i = 0; i < 100000; i++ ) {
        Slice key = Slice((char*)&i, sizeof(uint64_t));
        Slice value;
        ASSERT_FALSE(db->get(key, value)) << "get key " << i << " error";

        if (i % 10000 == 0) {
            cout << "read " << i << " records" << endl;
        }
    }

    delete db;
    delete opts.dir;
    delete opts.comparator;
}