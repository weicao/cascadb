#include <gtest/gtest.h>
#include "cascadb/slice.h"
#include "util/bloom.h"

using namespace std;
using namespace cascadb;

std::string filter_;
std::vector<std::string>  keys_;

static Slice build_key(int i, char *buffer)
{
    memcpy(buffer, &i, sizeof(i));
    return Slice(buffer, sizeof(uint32_t));
}

void add(const Slice& s)
{
    keys_.push_back(s.to_string());
}

void reset()
{
    keys_.clear();
    filter_.clear();
}

/*
 * build all keys bloom filter bitsets which in keys_ list
 */
void build()
{
    std::vector<Slice> key_slices;
    for (size_t i = 0; i < keys_.size(); i++) {
        key_slices.push_back(Slice(keys_[i]));
    }
    filter_.clear();
    cascadb::bloom_create(&key_slices[0], key_slices.size(), &filter_);
    keys_.clear();
}

/*
 * do match from the bloom filter bitsets
 */
bool matches(const Slice& s)
{
    if (!keys_.empty()) build();
    return cascadb::bloom_matches(s, filter_);
}

size_t filter_size(int n)
{
    return cascadb::bloom_size(n);
}

double false_positive_rate() 
{
    char buffer[sizeof(int)];
    int result = 0;
    for (int i = 0; i < 10000; i++) {
      if (matches(build_key(i + 1000000000, buffer))) {
        result++;
      }
    }
    return result / 10000.0;
}

static int nextlen(int length) 
{
    if (length < 10) {
        length += 1;
    } else if (length < 100) {
        length += 10;
    } else if (length < 1000) {
        length += 100;
    } else {
        length += 1000;
    }
    return length;
}

TEST(Bloom, emptyfilter) {
    ASSERT_TRUE(! matches("hello"));
    ASSERT_TRUE(! matches("world"));
}

TEST(Bloom, small) 
{
    add("hello");
    add("world");
    ASSERT_TRUE(matches("hello"));
    ASSERT_TRUE(matches("world"));
    ASSERT_TRUE(!matches("x"));
    ASSERT_TRUE(!matches("foo"));
}

TEST(Bloom, varying_lengths_and_check_postiverate) 
{
    double rate;
    char buffer[sizeof(int)];
    int mediocre_filters = 0;
    int good_filters = 0;

    for (int length = 1; length <= 10000; length = nextlen(length)) {
        reset();
        for (int i = 0; i < length; i++) {
            add(build_key(i, buffer));
        }
        build();

        ASSERT_EQ(filter_.size(), filter_size(length));

        // all added keys must match
        for (int i = 0; i < length; i++) {
            ASSERT_TRUE(matches(build_key(i, buffer)))
                << "Length " << length << "; key " << i;
        }

        // check false positive rate
        rate = false_positive_rate();
        fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                rate*100.0, length, static_cast<int>(filter_.size()));

        // must not be over 3.5%(leveldb is 2%, but its a hard hash
        ASSERT_LE(rate, 0.035);   

        // allowed, but not too often
        if (rate > 0.0125) 
            mediocre_filters++;
        else 
            good_filters++;
    }
    fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters, mediocre_filters);

    ASSERT_LE(mediocre_filters, good_filters / 6);
}
