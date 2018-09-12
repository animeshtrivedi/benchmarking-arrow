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
            if(BenchmarkConfiguration.testName.compareToIgnoreCase("ParquetToArrow") == 0){
                ParquetToArrow[] pq = new ParquetToArrow[BenchmarkConfiguration.parallel];
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    pq[i]  = new ParquetToArrow();
                    pq[i].setInputOutput(list[i]._1(), BenchmarkConfiguration.outputDir);
                    long tr = pq[i].process();
                    logger.info("\t [" +i+"] file " + list[i]._1()+ " contains record " + tr);
                }
            } else if (BenchmarkConfiguration.testName.compareToIgnoreCase("ArrowRead") == 0){
                // step 2: pass them to individual threads and initialize
                ArrowSingleFileReader[] allReaders = new ArrowSingleFileReader[BenchmarkConfiguration.parallel];
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    allReaders[i] = new ArrowSingleFileReader();
                    allReaders[i].init(list[i]._1());
                }
                long start = System.nanoTime();
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    allReaders[i].start();
                }
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    allReaders[i].join();
                }
                long end = System.nanoTime();

                long totalBytes = 0;
                for(int i =0; i < BenchmarkConfiguration.parallel; i++){
                    totalBytes+=allReaders[i].getTotalBytesRead();
                }
                double bandwidthGbps = totalBytes * 8 / (end - start);
                System.out.println("Total bytes: " + totalBytes + " bandwidth " + bandwidthGbps + " Gbps");
            } else {
                throw new Exception("Illegal test name: " + BenchmarkConfiguration.testName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
