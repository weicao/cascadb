#ifndef _CASCADB_SYS_LINUX_LINUX_FS_DIRECTORY_H_
#define _CASCADB_SYS_LINUX_LINUX_FS_DIRECTORY_H_

#include "sys/posix/posix_fs_directory.h"

namespace cascadb {

class LinuxFSDirectory : public PosixFSDirectory {
public:
    LinuxFSDirectory(const std::string& path) : PosixFSDirectory(path) {}

    virtual AIOFile* open_aio_file(const std::string& filename);

};

}

#endif