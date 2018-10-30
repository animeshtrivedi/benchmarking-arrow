//
// Created by atr on 30.10.18.
//

#include <string>
#include <sstream>
#include "BenchmarkResult.h"

long BenchmarkResult::totalBytesProcessed() {
    long size = 0;
    size+= totalInts() * 4;
    size+= totalLongs() * 8;
    size+= totalFloat4() * 4;
    size+= totalFloat8() * 8;
    size+= totalBinarySize();
    return size;
}

double BenchmarkResult::getBandwidthGbps() {
    long time = getRunTimeinNS();
    if(time > 0) {
        double bw = totalBytesProcessed();
        bw*=8;
        bw/=time;
        return bw;
    } else{
        return 0.0;
    }
}

std::string BenchmarkResult::summary(){
    std::stringstream ss;
    ss << "totalRows: " << totalRows() <<
           " || ints: " << totalInts() <<
           " , long " << totalLongs() <<
           " , float4 " << totalFloat4() <<
           " , double " << totalFloat8() <<
           " , binary " << totalBinary() <<
           " binarySize " << totalBinarySize() <<
           " checksum " << ((long) getChecksum()) <<
           " || runtimeInNS " << getRunTimeinNS() <<
           " , totalBytesProcessed " << totalBytesProcessed() <<
           " , bandwidth " << getBandwidthGbps() << " Gbps. \n";
    ss.flush();
    return ss.str();
}
