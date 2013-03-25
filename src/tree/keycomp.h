// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_KEY_COMP_H_
#define CASCADB_KEY_COMP_H_

#include <stdint.h>

#include "cascadb/slice.h"
#include "cascadb/comparator.h"

namespace cascadb {

// Compare keys between Msg, Record, Pivot and Slice
class KeyComp
{
public:
    KeyComp(Comparator* comp) : comp_(comp) {}

    template<typename T1, typename T2>
    bool operator() (const T1& left, const T2& right)
    {
        return comp_->compare(left.key, right.key) < 0;
    }

    template<typename T>
    bool operator() (const T& left, Slice right)
    {
        return comp_->compare(left.key, right) < 0;
    }

    template<typename T>
    bool operator() (Slice left, const T& right)
    {
        return comp_->compare(left, right.key) < 0;
    }

private:
    Comparator* comp_;
};

}

#endif
