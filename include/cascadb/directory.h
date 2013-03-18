#ifndef _CASCADB_DIRECTORY_H_
#define _CASCADB_DIRECTORY_H_

#include <string>

#include "file.h"

namespace cascadb {

class Directory {
public:
    virtual ~Directory() {}

    virtual bool file_exists(const std::string& filename) = 0;

    virtual SequenceFileReader* open_sequence_file_reader(const std::string& filename) = 0;
    
    virtual SequenceFileWriter* open_sequence_file_writer(const std::string& filename) = 0;

    virtual AIOFile* open_aio_file(const std::string& filename) = 0;

    virtual size_t file_length(const std::string& filename) = 0;

    virtual void rename_file(const std::string& from, const std::string& to) = 0;

    virtual void delete_file(const std::string& filename) = 0;

    virtual std::string to_string() = 0;
};

Directory* create_ram_directory();

Directory* create_fs_directory(const std::string& path);

}

#endif