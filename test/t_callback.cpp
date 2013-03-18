#include "util/callback.h"
#include <gtest/gtest.h>

using namespace std;
using namespace cascadb;

class ClassA {
public:
    ClassA() : value(0) {}

    void add(int a, int b) {
        value += a*b;
    }

    int value;
};

TEST(Callback, all) {
    ClassA a;

    Callback *cb = new Callback(&a, &ClassA::add, 2);
    cb->exec(3);
    delete cb;

    EXPECT_EQ(6, a.value);
}
