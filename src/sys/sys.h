#ifndef _CASCADB_SYS_SYS_H_
#define _CASCADB_SYS_SYS_H_

#include "sys/posix/posix_sys.h"

namespace cascadb {

class ScopedMutex {
public:
    ScopedMutex(Mutex *mu) : mu_(mu) {
        assert(mu_);
        mu_->lock();
        locked_ = true;
    }
    ~ScopedMutex() {
        if (locked_) {
            mu_->unlock();
        }
    }
    void lock() {
        mu_->lock();
        locked_ = true;
    }
    void unlock() {
        mu_->unlock();
        locked_ = false;
    }
private:
    ScopedMutex(const ScopedMutex&);
    ScopedMutex& operator =(const ScopedMutex&);
    bool locked_;
    Mutex *mu_;
};

}
#endif
