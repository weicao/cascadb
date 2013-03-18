#ifndef _CASCADB_TREE_FAST_VECTOR_H_
#define _CASCADB_TREE_FAST_VECTOR_H_

#include <vector>
#include <stdexcept>

namespace cascadb {

// A chain of small ordered vectors, each vector is guaranteed
// to have less elements than kVectorSizeLimit.
// This container provides fast insertion speed even when total
// size grows quite big.

template<typename T, int kVectorSizeLimit = 32>
class FastVector
{
protected:
    typedef std::vector<T> VectorType;
    typedef std::vector<VectorType*> ChainType;

    class Iterator {
    public:
        typedef Iterator self_type;
        typedef T value_type;
        typedef T& reference;
        typedef T* pointer;
        typedef std::forward_iterator_tag iterator_category;
        typedef int difference_type;

        Iterator() 
        : container_(NULL), 
          chain_idx_(0),
          vector_idx_(0)
        {
        }

        Iterator(const FastVector* container,
                 size_t chain_idx,
                 size_t vector_idx)
        : container_(container), 
          chain_idx_(chain_idx),
          vector_idx_(vector_idx)
        {
        }

        T &operator*()
        {
            assert(chain_idx_ != container_->chain_.size());
            VectorType *vec = container_->chain_[chain_idx_];
            assert(vector_idx_ != vec->size());
            return (*vec)[vector_idx_];
        }

        const T &operator*() const
        {
            assert(chain_idx_ != container_->chain_.size());
            VectorType *vec = container_->chain_[chain_idx_];
            assert(vector_idx_ != vec->size());
            return (*vec)[vector_idx_];
        }

        T *operator->()
        {
            assert(chain_idx_ != container_->chain_.size());
            VectorType *vec = container_->chain_[chain_idx_];
            assert(vector_idx_ != vec->size());
            return &((*vec)[vector_idx_]);
        }

        self_type &operator++()
        {
            assert(chain_idx_ != container_->chain_.size());
            VectorType *vec = container_->chain_[chain_idx_];
            assert(vector_idx_ != vec->size());
            vector_idx_ ++;
            if (vector_idx_ == vec->size()) {
                chain_idx_ ++;
                vector_idx_ = 0;
            }
            return *this;
        }

        self_type &operator++(int junk)
        {
            assert(chain_idx_ != container_->chain_.size());
            VectorType *vec = container_->chain_[chain_idx_];
            assert(vector_idx_ != vec->size());
            vector_idx_ ++;
            if (vector_idx_ == vec->size()) {
                chain_idx_ ++;
                vector_idx_ = 0;
            }
            return *this;
        }

        bool operator==(const self_type &other) const
        {
            if (container_ == other.container_ &&
                chain_idx_ == other.chain_idx_ &&
                vector_idx_ == other.vector_idx_)
                return true;
            return false;
        }

        bool operator!=(const self_type &other) const
        {
            return !(*this == other);
        }

    private:
        friend class FastVector;
        const FastVector *container_;
        size_t chain_idx_;
        size_t vector_idx_;
    };

public:
    typedef T           value_type;
    typedef T           *pointer;
    typedef const T     *const_pointer;
    typedef T           &reference;
    typedef const T     &const_reference;
    typedef Iterator    iterator;

public:
    FastVector()
    : size_(0)
    {
    }

    ~FastVector()
    {
        clear();
    }

    size_t size() const
    {
        return size_;
    }

    bool empty() const
    {
        return size_ == 0;
    }

    T &operator[](size_t index)
    {
        assert(index < size_);
        for (size_t i = 0; i < chain_.size(); i++ ) {
            if (index < chain_[i]->size()) {
                return chain_[i]->at(index);
            }
            index -= chain_[i]->size();
        }
        assert(false);
        throw std::runtime_error("fast_vector bad index");
    }

    const T &operator[](size_t index) const
    {
        assert(index < size_);
        for (size_t i = 0; i < chain_.size(); i++ ) {
            if (index < chain_[i]->size()) {
                return chain_[i]->at(index);
            }
            index -= chain_[i]->size();
        }
        assert(false);
        throw std::runtime_error("fast_vector bad index");
    }

    T &at(size_t index)
    {
        return (*this)[index];
    }

    void clear()
    {
        for (size_t i = 0; i < chain_.size(); i++ ) {
            delete chain_[i];
        }
        chain_.clear();
        size_ = 0;
    }

    void swap(FastVector &other)
    {
        chain_.swap(other.chain_);
        size_t tmp = size_;
        size_ = other.size_;
        other.size_ = tmp;
    }

    Iterator begin()
    {
        return Iterator(this, 0, 0);
    }

    Iterator end()
    {
        return Iterator(this, chain_.size(), 0);
    }

    template<typename KeyType, typename Compare>
    Iterator lower_bound(const KeyType &key, Compare compare)
    {
        size_t chain_idx = find_vector(key, compare);
        if (chain_idx != chain_.size()) {
            VectorType *vec = chain_[chain_idx];
            // vec->back() >= key
            assert(!compare(vec->back(), key));
            size_t vector_idx = find_record(vec, key, compare);
            assert(vector_idx != vec->size());
            return Iterator(this, chain_idx, vector_idx);
        }

        return Iterator(this, chain_.size(), 0);
    }

    template<typename KeyType, typename Compare>
    Iterator lower_bound(Iterator it, const KeyType &key, Compare compare)
    {
        assert(it.container_ == this);
        assert(it.chain_idx_ <= chain_.size());

        for (size_t chain_idx = it.chain_idx_; 
            chain_idx < chain_.size(); chain_idx ++) {
            VectorType *vec = chain_[chain_idx];
            // vec->back() >= key
            if (!compare(vec->back(), key)) {
                size_t vector_idx = find_record(vec, key, compare);
                assert(vector_idx != vec->size());
                return Iterator(this, chain_idx, vector_idx);
            }
        }

        return Iterator(this, chain_.size(), 0);
    }

    void push_back(const T& t)
    {
        insert(end(), t);
    }

    // Return an iterator that points to the first of the newly inserted elements.
    Iterator insert(Iterator it, const T& t)
    {
        assert(it.container_ == this);
        assert(it.chain_idx_ <= chain_.size());

        VectorType *vec;
        if (it.chain_idx_ == chain_.size()) {
            // insert at end
            if (chain_.size() == 0) {
                vec = new VectorType();
                vec->reserve(kVectorSizeLimit);
                chain_.push_back(vec);
            } else {
                vec = chain_.back();
            }
            vec->push_back(t);

            it.chain_idx_ = chain_.size() - 1;
            it.vector_idx_ = vec->size() - 1;
        } else {
            vec = chain_[it.chain_idx_];
            assert(it.vector_idx_ <= vec->size());
            vec->insert(vec->begin() + it.vector_idx_, t);
        }
        size_ ++;

        if (vec->size() >= kVectorSizeLimit) {
            VectorType *vec2 = new VectorType();
            vec2->reserve(kVectorSizeLimit);

            vec2->resize(kVectorSizeLimit/2);
            std::copy(vec->begin() + kVectorSizeLimit/2, vec->end(), vec2->begin());
            vec->resize(kVectorSizeLimit/2);

            chain_.insert(chain_.begin() + (it.chain_idx_ + 1), vec2);
            if (it.vector_idx_ >= kVectorSizeLimit/2) {
                it.chain_idx_ ++;
                it.vector_idx_ -= kVectorSizeLimit/2;
            }
        }
        return it;
    }

private:
    template<typename KeyType, typename Compare>
    size_t find_vector(const KeyType& key, Compare compare) const
    {
        size_t first = 0;
        size_t last = chain_.size();

        // binary search in the chain
        while (first != last) {
            size_t middle = (first + last)/2;

            std::vector<T>* vec = chain_[middle];
            assert(vec && vec->size());

            // vec->back() < key
            if (compare(vec->back(), key)) {
                first = middle+1;
            } else {
                last = middle;
            }
        }
        return first;
    }

    template<typename KeyType, typename Compare>
    size_t find_record(VectorType *vec, const KeyType& key, Compare compare) const
    {
        size_t first = 0;
        size_t last = vec->size();

        // binary search in the vector
        while (first != last) {
            size_t middle = (first + last)/2;

            // vec->at(middle) < key
            if (compare(vec->at(middle), key)) {
                first = middle + 1;
            } else {
                last = middle;
            }
        }
        return first;
    }

    ChainType       chain_;
    size_t          size_;
};

}


#endif
