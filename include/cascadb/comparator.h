#ifndef _CASCADB_COMPARATOR_H_
#define _CASCADB_COMPARATOR_H_

#include <assert.h>
#include "slice.h"

namespace cascadb {

class Comparator {
public:
    virtual int compare(const Slice & s1, const Slice & s2) const = 0;
};

class LexicalComparator : public Comparator {
public:
    int compare(const Slice & s1, const Slice & s2) const
    {
        return s1.compare(s2);
    }
};

template<typename NumericType>
class NumericComparator : public Comparator {
public:
    int compare(const Slice & s1, const Slice & s2) const
    {
        assert(s1.size() == sizeof(NumericType) &&
               s2.size() == sizeof(NumericType));
        NumericType *n1 = (NumericType*)s1.data();
        NumericType *n2 = (NumericType*)s2.data();
        return *n1 - *n2;
    }
};

}

#endif
