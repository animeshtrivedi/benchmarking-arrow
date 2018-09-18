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

public class BenchmarkConfiguration {
    // 1 MB writing buffer size
    static int writeBufferSizeShift = 20;
    static int writeBufferSize = 1 << BenchmarkConfiguration.writeBufferSizeShift;

    // write to a Crail, HDFS or local file system?
    static String[] validDestinations ={"hdfs", "crail", "local"};
    static String destination = "hdfs";
    // max fixed-size byte array width
    static int maxByteWidth = 8;
    // which test to do
    static String[] validTests ={"parquetToArrow", "arrowRead"};
    static String testName = "parquetToArrow";
    // input
    static String inputDir = null;
    static String outputDir = null;

    // number of parallel instances
    static int parallel = 1;

    static void setWriteBufferSize(int newSize){
        BenchmarkConfiguration.writeBufferSizeShift = (int) Math.ceil(Math.log(newSize)/Math.log(2));
        BenchmarkConfiguration.writeBufferSize = 1 << BenchmarkConfiguration.writeBufferSizeShift;
    }

    static void setWriteBufferShift(int shift){
        BenchmarkConfiguration.writeBufferSizeShift = shift;
        BenchmarkConfiguration.writeBufferSize = 1 << BenchmarkConfiguration.writeBufferSizeShift;
    }
}
