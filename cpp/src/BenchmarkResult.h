//
// Created by atr on 30.10.18.
//

#ifndef BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H
#define BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H


class BenchmarkResult {
protected:
    long _total_Ints;
    long _total_Longs;
    long _total_Float4;
    long _total_Float8;
    long _total_Binary;
    long _total_BinarySize;
    long _total_Rows;
    long _checksum;
    long _runtime_in_ns;
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
};

#endif //BENCHMARK_ARROW_CPP_BENCHMARKRESULT_H
