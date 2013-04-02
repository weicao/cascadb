#include "logger.h"
#include "compressor.h"

using namespace std;
using namespace cascadb;

#ifdef HAS_SNAPPY
#include <snappy.h>
#endif

size_t SnappyCompressor::max_compressed_length(size_t size)
{
#ifdef HAS_SNAPPY
    return snappy::MaxCompressedLength(size);
#else
    return 0;
#endif
}

bool SnappyCompressor::compress(const char *buf, size_t size, char *obuf, size_t *sp)
{
#ifdef HAS_SNAPPY
    snappy::RawCompress(buf, size, obuf, sp);
    return true;
#else
    return false;
#endif
}

bool SnappyCompressor::uncompress(const char *buf, size_t size, char *obuf)
{
#ifdef HAS_SNAPPY
    if (!snappy::RawUncompress(buf, size, obuf)) {
        LOG_ERROR("snappy uncompress error");
        return false;
    }
    return true;
#else
    return false;
#endif
}
