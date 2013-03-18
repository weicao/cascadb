#include <gtest/gtest.h>
#include "tree/fast_vector.h"

using namespace cascadb;
using namespace std;

TEST(FastVector, sequential_insert) {
    FastVector<int> vec;
    for (size_t i = 0; i < 1000; i++) {
        FastVector<int>::iterator it = vec.insert(vec.end(), i);
        ASSERT_EQ(i, (size_t)*it);
    }
    ASSERT_EQ(1000U, vec.size());

    FastVector<int>::iterator it = vec.begin();
    for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(i, (size_t)*it);
        it ++;
    }
    ASSERT_EQ(it, vec.end());
}


TEST(FastVector, lower_bound) {
    FastVector<int> vec;
    for (size_t i = 0; i < 1000; i++) {
        FastVector<int>::iterator it = vec.insert(vec.end(), i);
        ASSERT_EQ(i, (size_t)*it);
    }
    ASSERT_EQ(1000U, vec.size());

    std::less<int> compare;
    for (size_t i = 0; i < 1000; i++) {
        FastVector<int>::iterator it = vec.lower_bound(i, compare);
        ASSERT_EQ(i, (size_t)*it);
    }
}

TEST(FastVector, random_insert) {
    FastVector<int> vec;
    for (size_t i = 0; i < 1000; i++) {
        if (i % 100 == 0) continue;

        FastVector<int>::iterator it = vec.insert(vec.end(), i);
        ASSERT_EQ(i, (size_t)*it);
    }
    ASSERT_EQ(990U, vec.size());

    std::less<int> compare;
    for (size_t i = 0; i < 10; i++) {
        size_t k = i*100;
        FastVector<int>::iterator it = vec.lower_bound(k, compare);
        ASSERT_EQ(k + 1, (size_t)*it);
        it = vec.insert(it, i*100);
        ASSERT_EQ(k, (size_t)*it);
    }
    ASSERT_EQ(1000U, vec.size());

    FastVector<int>::iterator it = vec.begin();
    for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(i, (size_t)*it);
        it ++;
    }
    ASSERT_EQ(it, vec.end());
}

TEST(FastVector, index) {
    FastVector<int> vec;
    for (size_t i = 0; i < 1000; i++) {
        FastVector<int>::iterator it = vec.insert(vec.end(), i);
        ASSERT_EQ(i, (size_t)*it);
    }
    ASSERT_EQ(1000U, vec.size());

    for (size_t i = 0; i < 1000; i++) {
        ASSERT_EQ(i, (size_t)vec[i]);
    }
}