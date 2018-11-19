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

#ifndef BENCHMARK_ARROW_CPP_PEAKPERFORMANCEJAVA_H
#define BENCHMARK_ARROW_CPP_PEAKPERFORMANCEJAVA_H


#include <cstdint>
#include <iostream>
#include "BenchmarkResult.h"


class PeakPerformanceJava : public BenchmarkResult {
private:
    long items;
    int loop;
    uint8_t *bitmapBuffer;
    uint8_t* intValueBuffer;

public:
    PeakPerformanceJava() : PeakPerformanceJava(100000000, true, 1000000, 100) {
    }

    PeakPerformanceJava(int items, bool doNulls, int steps, int loop) {
        this->loop = loop;
        this->items = (long) items;
        int intSize = items << 2;
        int bitmapSize = items >> 3;
        bitmapBuffer = new uint8_t[bitmapSize];
        intValueBuffer = new uint8_t[intSize];

        std::cout<< items << " items = buffers of int size " << intSize << " bitmap size " << bitmapSize << " allocated \n";
        for(int i = 0; i < bitmapSize; i++){
            bitmapBuffer[i] = 0xFF;
        }
        // mark doNull
        for(int i = 0; doNulls && i < items; i+=steps){
            bitmapBuffer[(i>>3)] = 0xEF;
        }
        for(int i = 0; i < items; i++){
            intValueBuffer[i<<2] = i;
        }
        std::cout << "initialization done";
    }

    void run();
};


#endif //BENCHMARK_ARROW_CPP_PEAKPERFORMANCEJAVA_H
