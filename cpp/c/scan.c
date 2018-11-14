#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>


static uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

int isValid(const uint8_t *bitmap, long index) { 
   return (bitmap[index >>3 ] & kBitmask[index & 0x00000007LL]) != 0;
}

int main() {
  struct timespec start, end;
  long ints = 0, checksum = 0;
  long items = 10000000000L;
  int *data = (int*) malloc(items * 4);
  uint8_t *bitmap = (uint8_t*) malloc(items); // divide by 8, muiltiply by 8 then
 printf("items are  %lu data is at : %p and bitmap at %p \n", items, data,
         bitmap);
  for(long i = 0 ; i < items; i++) {
      data[i] = i;
      bitmap[i]=0xFF; 
    }
  printf("initialization done\n");
  clock_gettime(CLOCK_MONOTONIC_RAW, &start);
  for (long i = 0; i < items; i++) {
    if (isValid(bitmap, i)) {
      ints++;
      checksum += data[i];
    }
  }
  clock_gettime(CLOCK_MONOTONIC_RAW, &end);
  long nano = (((long) end.tv_sec * 1.0e+9) + end.tv_nsec) - (((long) start.tv_sec * 1.0e+9) + start.tv_nsec) ;
  double bw = (double) (ints * 32) / (double) nano;
  printf("nanodiff is %lu \n", nano);
  printf("checksum is %lu and ints are %lu bw %f \n", checksum, ints, bw );
  return 0;
}
