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
package com.github.animeshtrivedi.anoc;

abstract public class BenchmarkResults extends Thread{
    abstract long totalInts();
    abstract long totalLongs();
    abstract long totalFloat8();
    abstract long totalFloat4();
    abstract long totalBinary();
    abstract long totalBinarySize();
    abstract double getChecksum();
    long getTotalBytesRead(){
        long size = 0;
        size+= totalInts() * Integer.BYTES;
        size+= totalLongs() * Long.BYTES;
        size+= totalFloat4() * Float.BYTES;
        size+=totalFloat8() * Double.BYTES;
        size+=totalBinarySize();
        return size;
    }
}
