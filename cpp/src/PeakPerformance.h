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

#ifndef BENCHMARK_ARROW_CPP_PEAKPERFORMANCE_H
#define BENCHMARK_ARROW_CPP_PEAKPERFORMANCE_H


#include <stdint-gcc.h>
#include <iostream>
#include "BenchmarkResult.h"

static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

class PeakPerformance : public BenchmarkResult {
private:
    long rows;
    long validRows;
    long bitmapSize;
    const int *data;
    const uint8_t *bitmap;
public:
    explicit PeakPerformance(long rows, long validValues){
        uint8_t *temp_bitmap;
        int *temp_data;

        this->rows = rows;
        this->bitmapSize = rows / 8;
        if((rows & 0x00000007L) != 0) {
            this->bitmapSize++;
        }
        std::cout << " Passed rows: " << this->rows << " bitmap size is " << this->bitmapSize << "\n";
        temp_data = new int[rows];
        temp_bitmap = new uint8_t[this->bitmapSize];
        for(long i = 0; i < rows;i++)
            temp_data[i] = i;
        for(long i = 0; i < this->bitmapSize; i++)
            temp_bitmap[i] = 0xFF;

        //assign
        bitmap = temp_bitmap;
        data = temp_data;
        std::cout << " Random data generated and assigned \n";
        validRows = validValues;
    }

    void run(){
        localFunc();
        classFunc();
    }
    void classFunc();
    void localFunc();
    inline void runWithDebug(){ run(); }
    inline void init(){}

    inline bool IsValid(int64_t i) const {
        return (rows == validRows || (this->bitmap[i >>3 ] & kBitmask[i & 0x00000007LL]) != 0);
        //PeakPerformance::GetBit(this->bitmap, i));
    }

    static inline bool GetBit(const uint8_t* bits, int64_t i) {
        return (bits[i >>3 ] & kBitmask[i & 0x00000007LL]) != 0;
        //return (bits[i >>3 ] & kBitmask[i & 0x00000007LL]) != 0;
        //return ((i >> 3) != 0);
    }
};


#endif //BENCHMARK_ARROW_CPP_PEAKPERFORMANCE_H
