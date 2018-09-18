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
package com.github.animeshtrivedi.benchmark;

abstract public class BenchmarkResults extends Thread {
    protected BenchmarkResults(){
        this.longCount = 0;
        this.intCount = 0;
        this.float4Count = 0;
        this.float8Count = 0;
        this.binaryCount = 0;
        this.binarySizeCount = 0;
        this.totalRows = 0;
        this.checksum = 0;
        this.runtimeInNS = 0;
    }
    protected long longCount;
    protected long intCount;
    protected long float4Count;
    protected long float8Count;
    protected long binaryCount;
    protected long binarySizeCount;
    protected double checksum;
    protected long totalRows;
    protected long runtimeInNS;

    long totalInts(){
        return intCount;
    }

    long totalLongs(){
        return longCount;
    }

    long totalFloat8(){
        return float8Count;
    }

    long totalFloat4() {
        return float4Count;
    }
    long totalBinary(){
        return binaryCount;
    }

    long totalBinarySize() {
        return binarySizeCount;
    }

    long totalRows(){
        return totalRows;
    }

    public double getChecksum(){
        return this.checksum;
    }

    long getRunTimeinNS() {
        return this.runtimeInNS;
    }

    long getTotalBytesProcessed(){
        long size = 0;
        size+= totalInts() * Integer.BYTES;
        size+= totalLongs() * Long.BYTES;
        size+= totalFloat4() * Float.BYTES;
        size+=totalFloat8() * Double.BYTES;
        size+=totalBinarySize();
        return size;
    }

    String getBandwidthGbps(){
        long time = getRunTimeinNS();
        if(time > 0) {
            double bw = getTotalBytesProcessed();
            bw*=8;
            bw/=time;
            return String.format("%.2f", bw);
        } else{
            return "NaN";
        }
    }

    String summary(){
        return "totalRows: " + totalRows() +
                " || ints: " + totalInts() +
                " , long " + totalLongs() +
                " , float4 " + totalFloat4() +
                " , double " + totalFloat8() +
                " , binary " + totalBinary() +
                " binarySize " + totalBinarySize() +
                " || runtimeInNS " + getRunTimeinNS() +
                " , totalBytesProcessed " + getTotalBytesProcessed() +
                " , bandwidth " + getBandwidthGbps() + " Gbps.";
    }
}
