#ifndef CASCADB_UTIL_COMPRESSOR_H_
#define CASCADB_UTIL_COMPRESSOR_H_

namespace cascadb {

// Interface of data compression and decompression.
class Compressor {
 public:
    virtual ~Compressor() {}

    virtual size_t max_compressed_length(size_t size) = 0;

    // obuf should be larger than max_compressed_length(size)
    virtual bool compress(const char *buf, size_t size, char *obuf, size_t *sp) = 0;

    // obuf should be larger than uncompressed length
    virtual bool uncompress(const char *buf, size_t size, char *obuf) = 0;
};

class SnappyCompressor : public Compressor {
public:
    size_t max_compressed_length(size_t size);

    bool compress(const char *buf, size_t size, char *obuf, size_t *sp);

    // obuf should be larger than uncompressed length
    bool uncompress(const char *buf, size_t size, char *obuf);
};

}

#endif