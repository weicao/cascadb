// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _CASCADB_SLICE_H_
#define _CASCADB_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <stdexcept>

namespace cascadb {

class Slice
{
public:
    Slice() : data_(""), size_(0) {}
    
    Slice(const char *s)
    : data_(s)
    {
        assert(data_ != NULL);
        size_ = strlen(s);
    }

    Slice(const char *s, size_t n)
    : data_(s), size_(n)
    {
    }

    Slice(const std::string& s)
    : data_(s.c_str()), size_(s.size())
    {
    }
    
    const char* data() const { return data_; }
    
    size_t size() const { return size_; }
    
    bool empty() const 
    {
        return size_ == 0;
    }

    void resize(size_t s)
    {
        size_ = s;
    }
    
    void clear() 
    {
        data_ = ""; size_ = 0;
    }

    char operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }
    
    int compare(const Slice& x) const {
        int common_prefix_len = (size_ < x.size_) ? size_ : x.size_;
        int r = memcmp(data_, x.data_, common_prefix_len);
        if (r == 0) {
            if (size_ < x.size_) r = -1;
            else if (size_ > x.size_) r = 1;
        }
        return r;
    }
    
    std::string to_string() const {
        return std::string(data_, size_);
    }
    
    Slice clone() const {
        assert(size_);
        char* buf = new char[size_];
        assert(buf);
        memcpy(buf, data_, size_);
        return Slice(buf, size_);
    }
    
    void destroy() {
        assert(size_);
        delete[] data_;
        clear();
    }
    
private:
    const char  *data_;        // start address of buffer
    uint32_t    size_;          // buffer length
};

inline bool operator==(const Slice& x, const Slice& y) {
    return x.compare(y) == 0;
}

inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);
}

inline bool operator<(const Slice& x, const Slice& y) {
    return x.compare(y) < 0;
}

}

#endif
