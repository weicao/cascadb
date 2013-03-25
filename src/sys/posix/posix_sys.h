// Copyright (c) 2013 The CascaDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef CASCADB_SYS_POSIX_SYS_H_
#define CASCADB_SYS_POSIX_SYS_H_

#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#include <ostream>

namespace cascadb {

class Thread {
public:
    typedef void* (*func)(void* arg);
    Thread(func f);
    ~Thread();
    void start(void* arg);
    void join();
private:
  Thread(const Thread&);
  Thread& operator =(const Thread&);
  func f_;
  pthread_t thr_;
  bool alive_;
};

class Mutex {
public:
    Mutex();
    ~Mutex();
    void lock();
    bool lock_try();
    bool lock_try(unsigned int millisec);
    void unlock();
    bool locked() { return locked_; }
private:
    friend class CondVar;
    Mutex(const Mutex&);
    Mutex& operator =(const Mutex&);
    bool locked_;
    pthread_mutex_t mu_;
};

class RWLock {
public:
    RWLock();
    ~RWLock();
    void read_lock();
    bool try_read_lock();
    bool try_read_lock(unsigned int millisec);
    
    void write_lock();
    bool try_write_lock();
    bool try_write_lock(unsigned int millisec);
   
    void unlock();
private:
    RWLock(const RWLock&);
    RWLock& operator =(const RWLock&);
    pthread_rwlock_t rwlock_;
};

class CondVar {
public:
    CondVar(Mutex* mu);
    ~CondVar();
    void wait();
    bool wait(unsigned int millisec);
    void notify();
    void notify_all();
private:
    CondVar(const CondVar&);
    CondVar& operator =(const CondVar&);
    pthread_cond_t cv_;
    Mutex* mu_;
};


typedef struct timeval Time;
typedef time_t Second;
typedef useconds_t USecond;

extern bool operator<(const Time& t1, const Time& t2);

extern Time now();
extern std::ostream& operator<<(std::ostream& os, const Time& t);
extern uint64_t now_micros();
extern void sleep(Second sec);
extern void usleep(USecond usec);
// t2 - t1
extern USecond interval_us(Time t1, Time t2);

}

#endif
