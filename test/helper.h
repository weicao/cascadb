#ifndef CASCADB_TEST_HELPER_H_
#define CASCADB_TEST_HELPER_H_

#define PUT(mb, k, v) \
{\
    (mb).write(Msg(Put, Slice(k).clone(), Slice(v).clone()));\
}

#define DEL(mb, k) \
{\
    (mb).write(Msg(Del, Slice(k).clone()));\
}

#define CHK_MSG(m, t, k, v) \
    EXPECT_EQ(t, (m).type);\
    EXPECT_EQ(k, (m).key);\
    EXPECT_EQ(v, (m).value);


#define CHK_REC(r, k, v)\
    EXPECT_EQ(k, (r).key);\
    EXPECT_EQ(v, (r).value);

#endif
