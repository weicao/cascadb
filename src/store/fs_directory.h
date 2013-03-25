// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_STORE_FS_DIRECTORY_H_
#define CASCADB_STORE_FS_DIRECTORY_H_

#include "cascadb/directory.h"

namespace cascadb {

class FSDirectory : public Directory {
public:
    FSDirectory(const std::string & dir);

    virtual ~FSDirectory();

    virtual bool file_exists(const std::string& filename) = 0;

    virtual SequenceFileReader* open_sequence_file_reader(const std::string& filename) = 0;

    virtual SequenceFileWriter* open_sequence_file_writer(const std::string& filename) = 0;

    virtual AIOFile* open_aio_file(const std::string& filename) = 0;

    virtual size_t file_length(const std::string& filename) = 0;

    virtual void rename_file(const std::string& from, const std::string& to);

    virtual void delete_file(const std::string& filename);

    virtual std::string to_string();

protected:
    virtual const std::string fullpath(const std::string & filename) = 0;

    std::string dir_;
};

}

#endif
