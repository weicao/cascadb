// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_DB_H_
#define _CASCADB_DB_H_

#include <ostream>

#include "slice.h"
#include "comparator.h"
#include "options.h"
#include "directory.h"

namespace cascadb {

class DB {
public:
    virtual ~DB() {}

    static DB* open(const std::string& name, const Options& options);

    virtual bool put(Slice key, Slice value) = 0;

    inline bool put(const std::string& key, const std::string& value)
    {
        return put(Slice(key), Slice(value));
    }
    
    virtual bool del(Slice key) = 0;

    inline bool del(const std::string& key)
    {
        return del(Slice(key));
    }
    
    virtual bool get(Slice key, Slice& value) = 0;

    inline bool get(const std::string& key, std::string& value)
    {
        Slice v;
        if (!get(Slice(key), v)) {
            return false;
        }
        value.assign(v.data(), v.size());
        v.destroy();
        return true;
    }

    virtual void flush() = 0;

    virtual void debug_print(std::ostream& out) = 0;
};

}

#endif
