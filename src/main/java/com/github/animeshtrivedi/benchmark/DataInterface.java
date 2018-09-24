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

import java.text.NumberFormat;
import java.util.Locale;

public interface DataInterface extends Runnable {
    public long totalInts();
    public long totalLongs();
    public long totalFloat8();
    public long totalFloat4();
    public long totalBinary();
    public long totalBinarySize();
    public long totalRows();
    public double getChecksum();
    long getRunTimeinNS();

    public default long getTotalBytesProcessed(){
        long size = 0;
        size+= totalInts() * Integer.BYTES;
        size+= totalLongs() * Long.BYTES;
        size+= totalFloat4() * Float.BYTES;
        size+=totalFloat8() * Double.BYTES;
        size+=totalBinarySize();
        return size;
    }

    public default String getBandwidthGbps(){
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

    public default String summary(){
        String x = NumberFormat.getNumberInstance(Locale.US).format(getRunTimeinNS());
        return "totalRows: " + totalRows() +
                " || ints: " + totalInts() +
                " , long " + totalLongs() +
                " , float4 " + totalFloat4() +
                " , double " + totalFloat8() +
                " , binary " + totalBinary() +
                " binarySize " + totalBinarySize() +
                " || runtimeInNS " + x +
                " , totalBytesProcessed " + getTotalBytesProcessed() +
                " , bandwidth " + getBandwidthGbps() + " Gbps.";
    }
}
