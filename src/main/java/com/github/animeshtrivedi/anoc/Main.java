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

import org.apache.log4j.Logger;
import scala.Tuple2;

public class Main {
    final static Logger logger = Logger.getLogger(Main.class);
    public static void main(String[] args) {
        System.out.println("Welcome to Parquet Benchmarking project");
        ParseOptions options = new ParseOptions();
        options.parse(args);
        try {
            // step 1: enumerate all files which are there and take top "parallel" items
            Tuple2<String, Object>[] list =
                    Utils.enumerateWithSize(BenchmarkConfiguration.inputDir);
            if(list.length < BenchmarkConfiguration.parallel){
                throw new Exception("Parallel request " + BenchmarkConfiguration.parallel +
                        " is more than the number of files " + list.length);
            }
            BenchmarkResults[] results = new BenchmarkResults[BenchmarkConfiguration.parallel];
            if(BenchmarkConfiguration.testName.compareToIgnoreCase("ParquetToArrow") == 0){
                for(int i =0; i < BenchmarkConfiguration.parallel; i++) {
                    ParquetToArrow temp = new ParquetToArrow();
                    temp.setInputOutput(list[i]._1(), BenchmarkConfiguration.outputDir);
                    results[i] = temp;
                }
            } else if (BenchmarkConfiguration.testName.compareToIgnoreCase("ArrowRead") == 0) {
                for(int i =0; i < BenchmarkConfiguration.parallel; i++) {
                    ArrowSingleFileReader temp = new ArrowSingleFileReader();
                    temp.init(list[i]._1());
                    results[i] = temp;
                }
            } else if (BenchmarkConfiguration.testName.compareToIgnoreCase("ArrowMemRead") == 0) {
                for(int i =0; i < BenchmarkConfiguration.parallel; i++) {
                    ArrowMemoryReader temp = new ArrowMemoryReader();
                    temp.setInputOutput(list[i]._1());
                    results[i] = temp;
                }
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    ((ArrowMemoryReader) results[i]).finishInit();
                }
            } else {
                throw new Exception("Illegal test name: " + BenchmarkConfiguration.testName);
            }
            System.out.println("Test prep finished, starting the execution now ...");
            long start = System.nanoTime();
            for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                results[i].start();
            }
            for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                results[i].join();
            }
            long end = System.nanoTime();

            long totalBytes = 0;
            for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                totalBytes+=results[i].getTotalBytesProcessed();;
                System.out.println("\t ["+i+"] " + results[i].summary());
            }
            String bandwidthGbps = String.format("%.2f", (((double)totalBytes * 8) / (end - start)));
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("Total bytes: " + totalBytes + "(" + Utils.sizeToSizeStr2(totalBytes) + ") bandwidth " + bandwidthGbps + " Gbps");
            System.out.println("-----------------------------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
