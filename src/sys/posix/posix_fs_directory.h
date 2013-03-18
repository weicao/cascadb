#ifndef _CASCADB_SYS_POSIX_POSIX_FS_DIRECTORY_H_
#define _CASCADB_SYS_POSIX_POSIX_FS_DIRECTORY_H_

#include "store/fs_directory.h"

namespace cascadb {

class PosixFSDirectory : public FSDirectory {
public:
    PosixFSDirectory(const std::string& path) : FSDirectory(path) {}

    virtual ~PosixFSDirectory();

    virtual bool file_exists(const std::string& filename);

    virtual SequenceFileReader* open_sequence_file_reader(const std::string& filename);

    virtual SequenceFileWriter* open_sequence_file_writer(const std::string& filename);

    // I'm implementing posix AIO here, however, it canbe overriden to support 
    // kernel-level AIO in Linux, Solaris etc.
    virtual AIOFile* open_aio_file(const std::string& filename);

    virtual size_t file_length(const std::string& filename);

protected:
    virtual const std::string fullpath(const std::string& filename);
};

}

#endif