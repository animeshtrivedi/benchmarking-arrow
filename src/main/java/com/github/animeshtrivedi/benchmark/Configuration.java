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

import com.github.animeshtrivedi.generator.GeneratorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Configuration {
    // 1 MB writing buffer size
    static int writeBufferSizeShift = 20;
    static int writeBufferSize = 1 << Configuration.writeBufferSizeShift;

    // max fixed-size byte array width
    static int maxByteWidth = 8;
    // which test to do
    static String testName = "datagen";
    static List<String> fileReadTests = Arrays.asList("parquettoarrow", "arrowread");
    static boolean isFileReadingInvolved = true;
    // input
    static String inputDir = null;
    static String outputDir = null;

    // number of parallel instances
    static int parallel = 1;

    static void setWriteBufferSize(int newSize){
        Configuration.writeBufferSizeShift = (int) Math.ceil(Math.log(newSize)/Math.log(2));
        Configuration.writeBufferSize = 1 << Configuration.writeBufferSizeShift;
    }

    static void setWriteBufferShift(int shift){
        Configuration.writeBufferSizeShift = shift;
        Configuration.writeBufferSize = 1 << Configuration.writeBufferSizeShift;
    }

    // data gen settings
    public static int numCols = 1;
    public static long rowsPerThread = 1000;
    public static int binSize = 1024;
    public static int stepping = 1000;
    public static int type = GeneratorFactory.INT_GENERATOR;

    public static boolean debug = false;
    public static boolean warmup = false;

    public static boolean verbose = false;
    public static boolean offHeap = false;
    public static boolean runGC = false;

    public static boolean useHolder = false;
}
