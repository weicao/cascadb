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
