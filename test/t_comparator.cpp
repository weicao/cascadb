#include <gtest/gtest.h>
#include "cascadb/comparator.h"

using namespace cascadb;
using namespace std;

TEST(Comparator, lexical) {
    LexicalComparator comp;
    EXPECT_TRUE(comp.compare(Slice("a"), Slice("ab")) < 0);
    EXPECT_TRUE(comp.compare(Slice("ab"), Slice("ab")) == 0);
    EXPECT_TRUE(comp.compare(Slice("ab"), Slice("a")) > 0);
}

TEST(Comparator, numeric) {
    NumericComparator<int> comp;
    int a = 100, b = 200;
    EXPECT_TRUE(comp.compare(Slice((char *)&a, sizeof(a)), Slice((char *)&b, sizeof(b))) < 0);
    b = 100;
    EXPECT_TRUE(comp.compare(Slice((char *)&a, sizeof(a)), Slice((char *)&b, sizeof(b))) == 0);
    a = 200;
    EXPECT_TRUE(comp.compare(Slice((char *)&a, sizeof(a)), Slice((char *)&b, sizeof(b))) > 0);
}
