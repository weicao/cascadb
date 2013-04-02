#include <gtest/gtest.h>
#include "util/compressor.h"

using namespace cascadb;
using namespace std;

const char *text =
  "Yet another write-optimized storage engine, "
  "using buffered B-tree algorithm inspired by TokuDB.";

#ifdef HAS_SNAPPY

TEST(SnappyCompressor, compress) {
    Compressor *compressor = new SnappyCompressor();

    size_t len1;
    char* str1 = new char[compressor->max_compressed_length(strlen(text))];
    EXPECT_TRUE(compressor->compress(text, strlen(text), str1, &len1));
    EXPECT_NE(len1, strlen(text));

    char *str2 = new char[strlen(text)];
    EXPECT_TRUE(compressor->uncompress(str1, len1, str2));

    EXPECT_TRUE(strncmp(text, str2, strlen(text)) == 0);
}

#endif