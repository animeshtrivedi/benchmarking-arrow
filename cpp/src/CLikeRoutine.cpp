/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "CLikeRoutine.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

int CLikeRoutine::run(){
    struct timespec start, end;
    long ints = 0, checksum = 0;
    long items = 10000000000L;
    int *data = (int*) malloc(items * 4);
    uint8_t *bitmap = (uint8_t*) malloc(items/8); // divide by 8 => number of bytes
    printf("[c] items are  %lu data is at : %p and bitmap at %p \n", items, data,
           bitmap);
    for(long i = 0 ; i < items; i++) {
        data[i] = i;
        bitmap[i/8]=0xFF;
    }
    printf("[c] initialization done\n");
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
    printf("[c] nanodiff is %lu \n", nano);
    printf("[c] checksum is %lu and ints are %lu bw %f \n", checksum, ints, bw );
    return 0;
}