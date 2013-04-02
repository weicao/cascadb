#include <gtest/gtest.h>
#include "serialize/block.h"

using namespace cascadb;
using namespace std;

TEST(Block, searialize)
{
    char buffer[4096];
    Block blk(Slice(buffer, 4096), 0, 0);
    
    BlockWriter bw(&blk);
    ASSERT_TRUE(bw.writeUInt8(1));
    ASSERT_TRUE(bw.writeUInt16(12345));
    ASSERT_TRUE(bw.writeUInt32(123456789));
    ASSERT_TRUE(bw.writeUInt64(123456789000000));
    Slice s1("abcdefg");
    Slice s2("hijklmn");
    ASSERT_TRUE(bw.writeSlice(s1));
    EXPECT_EQ("abcdefg", s1);
    ASSERT_TRUE(bw.writeSlice(s2));
    EXPECT_EQ("hijklmn",s2);
    
    BlockReader br(&blk);
    uint8_t a;
    uint16_t b;
    uint32_t c;
    uint64_t d;
    Slice e;
    Slice f;
    ASSERT_TRUE(br.readUInt8(&a));
    ASSERT_TRUE(br.readUInt16(&b));
    ASSERT_TRUE(br.readUInt32(&c));
    ASSERT_TRUE(br.readUInt64(&d));
    ASSERT_TRUE(br.readSlice(e));
    ASSERT_TRUE(br.readSlice(f));
    EXPECT_EQ(a, 1U);
    EXPECT_EQ(b, 12345U);
    EXPECT_EQ(c, 123456789U);
    EXPECT_EQ(d, 123456789000000U);
    EXPECT_EQ(e, "abcdefg");
    EXPECT_EQ(f, "hijklmn");
}

TEST(Block, writer_overflow)
{
    char buffer[4096];
    Block blk(Slice(buffer, 4096), 0, 0);
    BlockWriter bw(&blk);
    
    bw.seek(4095);
    EXPECT_TRUE(bw.writeUInt8(1));
    bw.seek(4096);
    EXPECT_FALSE(bw.writeUInt8(1));
    
    char data[4092];
    bw.seek(0);
    Slice s1(data, 4092);
    EXPECT_TRUE(bw.writeSlice(s1));
    bw.seek(1);
    Slice s2(data, 4092);
    EXPECT_FALSE(bw.writeSlice(s2));
}

TEST(Block, reader_overflow)
{
    char buffer[4096];
    Block blk(Slice(buffer, 4096), 0, 0);
    BlockReader br(&blk);
    BlockWriter bw(&blk);
    uint8_t n;
    
    EXPECT_TRUE(bw.writeUInt8(1));
    EXPECT_TRUE(br.readUInt8(&n));
    EXPECT_FALSE(br.readUInt8(&n));
    EXPECT_TRUE(bw.writeUInt8(2));
    EXPECT_TRUE(br.readUInt8(&n));
}
