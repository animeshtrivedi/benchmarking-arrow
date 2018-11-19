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

#include <chrono>
#include "PeakPerformance.h"
#if 0
void PeakPerformance::run() {
    std::cout << " version 1 \n";
    auto start = std::chrono::high_resolution_clock::now();
    // we walk through the array and materialize values
    for(long i = 0; i < this->rows; ){
        uint8_t flag = this->bitmap[i/8];
        for(long j = i; i < j + 8; i++) {
            if (flag & 0x01) {
                this->_total_Ints++;
                this->_checksum += this->data[i];
            }
            flag>>=1;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}
#endif

void PeakPerformance::localFunc() {
    this->_total_Ints = 0;
    this->_checksum = 0;
    this->_runtime_in_ns = 0;
    long totalInts=0, checksum=0;
    const long rowx = this->rows;
    const int *ptr = this->data;
    const uint8_t *b = this->bitmap;
    std::cout << " version 4 \n";
    auto start = std::chrono::high_resolution_clock::now();
    // we walk through the array and materialize values
    for(long i = 0; i < rowx; i++){
        //if (IsValid(i)) {
        //if (PeakPerformance::GetBit(b, i)){
        if (b[i >> 3L ] & ( 1L << (i & 0x7L))){
            totalInts++;
            checksum+=ptr[i];
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    this->_total_Ints = totalInts;
    this->_checksum = checksum;
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}


void PeakPerformance::classFunc() {
    auto start = std::chrono::high_resolution_clock::now();
    // we walk through the array and materialize values
    for(long i = 0; i < this->rows; i++){
        //if (IsValid(i)) {
        if (PeakPerformance::GetBit(this->bitmap, i)){
            this->_total_Ints++;
            this->_checksum+=this->data[i];
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
}