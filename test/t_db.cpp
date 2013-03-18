#include <iostream>

#include <gtest/gtest.h>

#include "cascadb/db.h"

using namespace std;
using namespace cascadb;

TEST(DB, read_and_write) {
    Options opts;
    opts.dir = create_ram_directory();
    opts.comparator = new NumericComparator<uint64_t>();
    opts.inner_node_page_size = 4 * 1024;
    opts.inner_node_children_number = 64;
    opts.leaf_node_page_size = 4 * 1024;
    opts.cache_limit = 32 * 1024;

    DB *db = DB::open("test_db", opts);
    EXPECT_TRUE(db != NULL);

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
    db->debug_print(cout);

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
