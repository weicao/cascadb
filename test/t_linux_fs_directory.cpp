#include "sys/linux/linux_fs_directory.h"

#include "directory_test.h"

class LinuxSequenceFileTest : public SequenceFileTest {
public:
    LinuxSequenceFileTest()
    {
        dir = new LinuxFSDirectory("/tmp");
    }
};

TEST_F(LinuxSequenceFileTest, read_and_write) {
    TestReadAndWrite();
}

class LinuxAIOFileTest : public AIOFileTest {
public:
    LinuxAIOFileTest() 
    {
        dir = new LinuxFSDirectory("/tmp");
    }
};

TEST_F(LinuxAIOFileTest, blocking_read_and_write)
{
    TestBlockingReadAndWrite();
}

TEST_F(LinuxAIOFileTest, read_and_write)
{
    TestReadAndWrite();
}

TEST_F(LinuxAIOFileTest, read_partial)
{
    TestReadPartial();
}
