// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

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