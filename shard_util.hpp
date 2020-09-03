#ifndef SHARD_UTIL_H
#define SHARD_UTIL_H
#include <stdlib.h>
/**
  0,34177280
  1,34699264
  2,35749888
  3,35749888
  4,34437120
  5,35696640
  6,30261152
*/
size_t MODEL_BATCHES[7] = {34177280UL, 34699264UL, 35749888UL, 35749888UL, 34437120UL, 35696640UL, 30261152UL};
int MODEL_BATCH_N = 7;

int COMM_NW = 4;
size_t COMM_SLICE = 1024 * 1024;

size_t totalSize(size_t model[], int size) {
  size_t total = 0;
  for (int i = 0; i < size; i++) {
    total += model[i];
  }
  return total;
}

#endif
