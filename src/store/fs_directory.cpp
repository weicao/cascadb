#include <stdio.h>

#include "store/fs_directory.h"

using namespace std;
using namespace cascadb;

FSDirectory::FSDirectory(const string& dir) : dir_(dir)
{
}

FSDirectory::~FSDirectory()
{
}

void FSDirectory::rename_file(const std::string& from, const std::string& to)
{
    rename(fullpath(from).c_str(), fullpath(to).c_str());
}

void FSDirectory::delete_file(const std::string& filename)
{
    remove(fullpath(filename).c_str());
}

std::string FSDirectory::to_string()
{
    return "FSDirectory:@path=" + dir_;    
}

#ifdef OS_LINUX
#include "sys/linux/linux_fs_directory.h"
#endif

#include "sys/posix/posix_fs_directory.h"

Directory* cascadb::create_fs_directory(const std::string& path)
{
#ifdef OS_LINUX
    return new LinuxFSDirectory(path);
#else
    return new PosixFSDirectory(path);
#endif
}