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


#ifndef BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H
#define BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H


class BenchmarkResult {
protected:
    //cache line 1
    long _runtime_in_ns;
    long _checksum;
    long _total_Ints;
    long _total_Longs;
    long _total_Float4;
    long _total_Float8;
    long _total_Binary;
    long _total_BinarySize;
    // cache line 2
    long _total_Rows;

public:
    BenchmarkResult(){
        _total_Ints = 0;
        _total_Longs  = 0;
        _total_Float4  = 0;
        _total_Float8 = 0;
        _total_Binary = 0;
        _total_BinarySize = 0;
        _total_Rows = 0;
        _checksum = 0;
        _runtime_in_ns = 0 ;
    }
    virtual ~BenchmarkResult(){}
    long totalInts() {
        return _total_Ints;
    }
    long totalLongs(){
        return _total_Longs;
    }
    long totalFloat8(){
        return _total_Float8;
    }
    long totalFloat4() {
        return _total_Float4;
    }
    long totalBinary(){
        return _total_Binary;
    }
    long totalBinarySize(){
        return _total_BinarySize;
    }
    long totalRows(){
        return _total_Rows;
    }
    double getChecksum(){
        return _checksum;
    }
    long getRunTimeinNS(){
        return _runtime_in_ns;
    }

    long totalBytesProcessed();
    double getBandwidthGbps();
    std::string summary();

    void run(){}
    void runWithDebug(){}
    void init(){}
};

#endif //BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H
