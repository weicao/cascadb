#include <gtest/gtest.h>
#include "sys/sys.h"

using namespace cascadb;
using namespace std;

int count1;
void *body1(void* arg)
{
    count1 ++;
    return NULL;
}

TEST(Thread, run) {
    Thread thr(body1);
    count1 = 0;
    thr.start(NULL);
    thr.join();
    EXPECT_EQ(count1, 1);
}

Mutex mu2;
void *body2(void* arg)
{
    mu2.lock();
    cascadb::usleep(100000); // 100 ms
    mu2.unlock();
    return NULL;
}  

TEST(Mutex, lock) {
    Thread thr1(body2);
    Thread thr2(body2);
    
    Time t1 = now();
    
    thr1.start(NULL);
    thr2.start(NULL);
    thr1.join();
    thr2.join();
    
    Time t2 = now();
    
    EXPECT_NEAR(200000, interval_us(t1, t2), 5000);
}

TEST(Mutex, trylock) {
    Thread thr(body2);
    thr.start(NULL);
    cascadb::usleep(1000);
    
    EXPECT_FALSE(mu2.lock_try());
    cascadb::usleep(100000);
    EXPECT_TRUE(mu2.lock_try());
    mu2.unlock();
    
    thr.join();
}

TEST(Mutex, timedlock) {
    Thread thr(body2);
    thr.start(NULL);
    cascadb::usleep(1000);
    
    EXPECT_FALSE(mu2.lock_try());
    EXPECT_FALSE(mu2.lock_try(50));
    EXPECT_TRUE(mu2.lock_try(50));
    mu2.unlock();
    
    thr.join();
}

int count;
Mutex mu3;
CondVar cond3(&mu3);
void *body3(void* arg)
{
    cascadb::usleep(100000); // 100 ms
    mu3.lock();
    count ++;
    mu3.unlock();
    cond3.notify();
    return NULL;
}

TEST(CondVar, wait) {
    count = 0;
    
    Thread thr(body3);
    thr.start(NULL);

    mu3.lock();
    cond3.wait();
    EXPECT_EQ(count, 1);
    mu3.unlock();
    
    thr.join();
}

TEST(CondVar, timedwait) {
    count = 0;
    
    Thread thr(body3);
    thr.start(NULL);

    mu3.lock();
    cond3.wait(50);
    EXPECT_EQ(count, 0);
    cond3.wait(51);
    EXPECT_EQ(count, 1);
    mu3.unlock();
    
    thr.join();
}

RWLock rwl;

void *body4(void* arg)
{
    rwl.read_lock();
    cascadb::usleep(100000); // 100 ms
    rwl.unlock();
    
    return NULL;
}

TEST(RWLock, rdlock) {
    Thread thr1(body4);
    thr1.start(NULL);

    Thread thr2(body4);
    thr2.start(NULL);
    
    cascadb::usleep(10000);
    EXPECT_TRUE(rwl.try_read_lock());
    rwl.unlock();
    
    EXPECT_FALSE(rwl.try_write_lock());
    cascadb::usleep(100000); // 100 ms
    
    EXPECT_TRUE(rwl.try_write_lock());
    rwl.unlock();
    
    thr1.join();
    thr2.join();
}
