#include <gtest/gtest.h>
#include "tree/msg.h"
#include "helper.h"

using namespace cascadb;
using namespace std;

TEST(Msg, searialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);
    
    Msg m1;
    m1.set_put(Slice("put"), Slice("value"));
    ASSERT_TRUE(m1.write_to(writer));
    
    Msg m2;
    m2.set_del(Slice("del"));
    ASSERT_TRUE(m2.write_to(writer));
    
    EXPECT_TRUE(blk.size() == m1.size() + m2.size());
    
    Msg m3;
    ASSERT_TRUE(m3.read_from(reader));
    EXPECT_EQ(Put, m3.type);
    EXPECT_EQ("put", m3.key);
    EXPECT_EQ("value", m3.value);
    
    Msg m4;
    ASSERT_TRUE(m4.read_from(reader));
    EXPECT_EQ(Del, m4.type);
    EXPECT_EQ("del", m4.key);
    EXPECT_EQ(Slice(), m4.value);

    EXPECT_TRUE(blk.size() == m3.size() + m4.size());
    EXPECT_EQ(0U ,reader.remain());
}

TEST(MsgBuf, write)
{
    LexicalComparator comp;
    MsgBuf mb(&comp);
    
    PUT(mb, "abc", "1");
    PUT(mb, "aaa", "1");
    PUT(mb, "b", "1");
    PUT(mb, "aaa", "2");
    PUT(mb, "c", "1");
    PUT(mb, "b", "1");
    PUT(mb, "b", "2");
    PUT(mb, "b", "3");
    DEL(mb, "c");

    EXPECT_EQ(4U, mb.count());
    
    CHK_MSG(mb.get(0), Put, "aaa", "2");
    CHK_MSG(mb.get(1), Put, "abc", "1");
    CHK_MSG(mb.get(2), Put, "b", "3");
    CHK_MSG(mb.get(3), Del, "c", Slice());
}

TEST(MsgBuf, append)
{
    LexicalComparator comp;
    MsgBuf mb1(&comp);
    MsgBuf mb2(&comp);

    PUT(mb1, "abc", "1");
    PUT(mb1, "aaa", "1");
    PUT(mb1, "b", "1");
    PUT(mb1, "aaa", "2");
    PUT(mb1, "c", "1");
   
    PUT(mb2, "b", "1");
    PUT(mb2, "b", "2");
    PUT(mb2, "b", "3");
    DEL(mb2, "c");
    
    mb1.append(mb2.begin(), mb2.end());
    mb2.clear();

    EXPECT_EQ(4U, mb1.count());
    
    CHK_MSG(mb1.get(0), Put, "aaa", "2");
    CHK_MSG(mb1.get(1), Put, "abc", "1");
    CHK_MSG(mb1.get(2), Put, "b", "3");
    CHK_MSG(mb1.get(3), Del, "c", Slice());
}

TEST(MsgBuf, find)
{
    LexicalComparator comp;
    MsgBuf mb(&comp);

    PUT(mb, "abc", "1");
    PUT(mb, "aaa", "1");
    PUT(mb, "b", "1");
    PUT(mb, "aaa", "2");
    PUT(mb, "c", "1");
    PUT(mb, "b", "1");
    PUT(mb, "b", "2");
    PUT(mb, "b", "3");
    DEL(mb, "c");
   
    CHK_MSG(*mb.find("a"), Put, "aaa", "2");
    CHK_MSG(*mb.find("aaa"), Put, "aaa", "2");
    CHK_MSG(*mb.find("abc"), Put, "abc", "1");
    CHK_MSG(*mb.find("b"), Put, "b", "3");
    CHK_MSG(*mb.find("c"), Del, "c", Slice());
    EXPECT_TRUE(mb.end() == (mb.find("d")));
}

TEST(MsgBuf, searialize)
{
    char buffer[4096];
    Block blk(buffer, 0, 4096);
    BlockReader reader(&blk);
    BlockWriter writer(&blk);
    
    LexicalComparator comp;
    MsgBuf mb1(&comp);
    
    PUT(mb1, "a", "1");
    DEL(mb1, "b");
    
    mb1.write_to(writer);
    
    EXPECT_EQ(mb1.size(), blk.size());
    
    MsgBuf mb2(&comp);
    mb2.read_from(reader);
    
    EXPECT_EQ(2U, mb2.count());
    CHK_MSG(mb2.get(0), Put, "a", "1");
    CHK_MSG(mb2.get(1), Del, "b", Slice());

    EXPECT_EQ(mb2.size(), blk.size());
}
