#include "store/ram_directory.h"

#include "directory_test.h"

class RAMSequenceFileTest : public SequenceFileTest {
public:
    RAMSequenceFileTest()
    {
        dir = new RAMDirectory();
    }
};

TEST_F(RAMSequenceFileTest, read_and_write) {
    TestReadAndWrite();
}

class RAMAIOFileTest : public AIOFileTest {
public:
    RAMAIOFileTest() 
    {
        dir = new RAMDirectory();
    }
};

TEST_F(RAMAIOFileTest, blocking_read_and_write)
{
    TestBlockingReadAndWrite();
}

TEST_F(RAMAIOFileTest, read_and_write)
{
    TestReadAndWrite();
}

TEST_F(RAMAIOFileTest, read_partial)
{
    TestReadPartial();
}
