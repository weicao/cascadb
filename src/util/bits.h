#ifndef _CASCADB_UTIL_BITS_H_
#define _CASCADB_UTIL_BITS_H_

#define ROUND_UP(x, n)  ((((x)+ (n) - 1) / (n)) * (n))
#define ROUND_DOWN(x, n)  (((x) / (n)) * (n))

#define PAGE_SIZE 4096
#define PAGE_ROUND_UP(x) (((x) + PAGE_SIZE-1)  & (~(PAGE_SIZE-1)))
#define PAGE_ROUND_DOWN(x) ((x) & (~(PAGE_SIZE-1)))
#define PAGE_ROUNDED(x) ((x) == PAGE_ROUND_DOWN(x) )

#endif
