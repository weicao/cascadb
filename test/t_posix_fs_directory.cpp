#include "sys/posix/posix_fs_directory.h"

#include "directory_test.h"

class PosixSequenceFileTest : public SequenceFileTest {
public:
    PosixSequenceFileTest()
    {
        dir = new PosixFSDirectory("/tmp");
    }
};

TEST_F(PosixSequenceFileTest, read_and_write) {
    TestReadAndWrite();
}

class PosixAIOFileTest : public AIOFileTest {
public:
    PosixAIOFileTest() 
    {
        dir = new PosixFSDirectory("/tmp");
    }
};

TEST_F(PosixAIOFileTest, blocking_read_and_write)
{
    TestBlockingReadAndWrite();
}

TEST_F(PosixAIOFileTest, read_and_write)
{
    TestReadAndWrite();
}

TEST_F(PosixAIOFileTest, read_partial)
{
    TestReadPartial();
}
