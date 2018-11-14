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

#ifndef BENCHMARK_ARROW_CPP_CLIKEROUTINE_H
#define BENCHMARK_ARROW_CPP_CLIKEROUTINE_H

#include <stdint.h>

static uint8_t _kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

class CLikeRoutine {
public:

    int inline isValid(const uint8_t *bitmap, long index){
        return (bitmap[index >>3 ] & _kBitmask[index & 0x00000007LL]) != 0;
    }

    CLikeRoutine(){}
    int run();
};
#endif //BENCHMARK_ARROW_CPP_CLIKEROUTINE_H
