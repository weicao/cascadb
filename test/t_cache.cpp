#include <gtest/gtest.h>

#include "cascadb/options.h"
#include "sys/sys.h"
#include "store/ram_directory.h"
#include "serialize/layout.h"
#include "cache/cache.h"

using namespace std;
using namespace cascadb;

class FakeNode : public Node {
public:
    FakeNode(const std::string& table_name, bid_t nid) : Node(table_name, nid), data(0) {}

    bool cascade(MsgBuf *mb, InnerNode* parent) { return false; }

    bool find(Slice key, Slice& value, InnerNode* parent) { return false; }

    void lock_path(Slice key, std::vector<Node*>& path) {}
    
    size_t size()
    {
        return 4096;
    }

    size_t estimated_buffer_size()
    {
        return size();
    }
    
    bool read_from(BlockReader& reader, bool skeleton_only) {
        EXPECT_TRUE(reader.readUInt64(&data));
        Slice s;
        EXPECT_TRUE(reader.readSlice(s));
        return true;
    }

    bool write_to(BlockWriter& writer, size_t& skeleton_size) {
        EXPECT_TRUE(writer.writeUInt64(nid_));
        char buf[4084] = {0};
        Slice s(buf, sizeof(buf));
        EXPECT_TRUE(writer.writeSlice(s));
        skeleton_size = size();
        return true;
    }

    uint64_t data;
};

class FakeNodeFactory : public NodeFactory {
public:
    FakeNodeFactory(const std::string& table_name)
    : table_name_(table_name)
    {
    }

    Node* new_node(bid_t nid) {
        return new FakeNode(table_name_, nid);
    }

    std::string table_name_;
};

TEST(Cache, read_and_write) {
    Options opts;
    opts.cache_limit = 4096 * 1000;

    Directory *dir = new RAMDirectory();
    AIOFile *file = dir->open_aio_file("cache_test");
    Layout *layout = new Layout(file, 0, opts);
    layout->init(true);

    Cache *cache = new Cache(opts);
    cache->init();

    NodeFactory *factory = new FakeNodeFactory("t1");
    cache->add_table("t1", factory, layout);

    for (int i = 0; i < 1000; i++) {
        Node *node = new FakeNode("t1", i);
        node->set_dirty(true);
        cache->put("t1", i, node);
        node->dec_ref();
    }

    // give it time to flush
    cascadb::sleep(5);
    // flush rest and clear nodes
    cache->del_table("t1");

    cache->add_table("t1", factory, layout);
    for (int i = 0; i < 1000; i++) {
        Node *node = cache->get("t1", i, false);
        EXPECT_TRUE(node != NULL);
        EXPECT_EQ((uint64_t)i, ((FakeNode*)node)->data);
        node->dec_ref();
    }
    cache->del_table("t1");

    delete cache;
    delete factory;
    delete layout;
    delete file;
    delete dir;
}