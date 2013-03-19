// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_DB_IMPL_H_
#define _CASCADB_DB_IMPL_H_

#include "cascadb/db.h"
#include "serialize/layout.h"
#include "cache/cache.h"
#include "tree/tree.h"

namespace cascadb {

class Tree;

class DBImpl : public DB {
public:
    DBImpl(const std::string& name, const Options& options)
    : name_(name), options_(options),
      file_(NULL), layout_(NULL),
      cache_(NULL), tree_(NULL), node_store_(NULL)
    {
    }
    
    ~DBImpl();
    
    bool init();

    bool put(Slice key, Slice value);
    
    bool del(Slice key);
    
    bool get(Slice key, Slice& value);

    void flush();

    void debug_print(std::ostream& out);

private:
    std::string name_;
    Options options_;
    
    AIOFile *file_;
    Layout *layout_;
    Cache *cache_;
    Tree* tree_;
    NodeStore *node_store_;
};

}

#endif
