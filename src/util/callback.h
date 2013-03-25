// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_UTIL_CALLBACK_H_
#define CASCADB_UTIL_CALLBACK_H_

#include <assert.h>
#include <deque>
#include "any.h"

namespace cascadb {

template<typename A>
class CallbackBase : public Any {
public:
    virtual ~CallbackBase() {}
    virtual void exec(A) = 0;
};

template<typename T, typename C, typename A>
class Bind : public CallbackBase<A> {
public:
    typedef void (T::*F)(C, A);

    Bind(T *target, F function, C context)
    : target_(target), function_(function), context_(context)
    {
    }

    void exec(A arg) {
        assert(target_);
        (target_->*function_)(context_, arg);
    }

private:
    T               *target_;
    F               function_;
    C               context_;
};

class Callback {
public:
    template<typename T, typename C, typename A>
    Callback(T *target, void (T::*function)(C, A), C context) {
        base_ = new Bind<T, C, A>(target, function, context);
    }

    ~Callback() {
        delete base_;
    }

    template<typename A>
    void exec(A arg) {
        CallbackBase<A> *cbp = (CallbackBase<A> *) base_;
        cbp->exec(arg);
    }

private:
    Any *base_;
};

}

#endif