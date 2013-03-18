#include <gtest/gtest.h>
#include "tree/keycomp.h"
#include "tree/msg.h"
#include "tree/node.h"

using namespace cascadb;
using namespace std;

TEST(Query, compare)
{
    LexicalComparator c;
    KeyComp comp(&c);

    // msg to string 
    EXPECT_TRUE(comp(Msg(Put, "a", "1"), Slice("b") ));
    EXPECT_FALSE(comp(Slice("b"), Msg(Put, "a", "1")));

    // record to string
    EXPECT_TRUE(comp(Record("a", "1"), Slice("b") ));
    EXPECT_FALSE(comp(Slice("b"), Record("a", "1")));

    // record to msg 
    EXPECT_TRUE(comp(Record("a", "1"), Msg(Put, "b", "1")));
    EXPECT_FALSE(comp(Msg(Put, "b", "1"), Record("a", "1")));
}
