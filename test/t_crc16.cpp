#include <gtest/gtest.h>
#include "cascadb/slice.h"
#include "util/crc16.h"

using namespace cascadb;
using namespace std;

TEST(crc16, calc) 
{
    Slice s("Hello World");

    EXPECT_EQ(cascadb::crc16(s.data(), 11), cascadb::crc16(s.data(), 11));
}
