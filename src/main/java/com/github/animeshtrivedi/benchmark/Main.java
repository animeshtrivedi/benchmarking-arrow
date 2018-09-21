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

import com.github.animeshtrivedi.generator.BinaryGenerator;
import com.github.animeshtrivedi.generator.GeneratorFactory;
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
                    Utils.enumerateWithSize(Configuration.inputDir);
            if (Configuration.isFileReadingInvolved && list.length < Configuration.parallel) {
                throw new Exception("Parallel request " + Configuration.parallel +
                        " is more than the number of files " + list.length);
            }
            DataInterface[] ops = new DataInterface[Configuration.parallel];

            if (Configuration.testName.compareToIgnoreCase("datagen") == 0) {
                if (Configuration.type == GeneratorFactory.INT_GENERATOR) {
                    for (int i = 0; i < Configuration.parallel; i++) {
                        HDFSWritableByteChannel w = new HDFSWritableByteChannel(Configuration.destination+i);
                        BinaryGenerator temp = new BinaryGenerator(w);
                        ops[i] = temp;
                    }
                } else if (Configuration.type == GeneratorFactory.BIN_GENERATOR) {
                    for (int i = 0; i < Configuration.parallel; i++) {
                        HDFSWritableByteChannel w = new HDFSWritableByteChannel("/datagen/arrow/output"+i);
                        BinaryGenerator temp = new BinaryGenerator(w);
                        ops[i] = temp;
                    }
                } else {
                    throw new Exception("datagen type " + Configuration.type + " not implemented");
                }
            } else if (Configuration.testName.compareToIgnoreCase("ParquetToArrow") == 0) {
                for (int i = 0; i < Configuration.parallel; i++) {
                    ParquetToArrow temp = new ParquetToArrow();
                    temp.setInputOutput(list[i]._1(), Configuration.outputDir);
                    ops[i] = temp;
                }
            } else if (Configuration.testName.compareToIgnoreCase("ArrowRead") == 0) {
                for (int i = 0; i < Configuration.parallel; i++) {
                    ArrowReaderDebug temp = new ArrowReaderDebug();
                    temp.init(list[i]._1());
                    ops[i] = temp;
                }
            } else if (Configuration.testName.compareToIgnoreCase("ArrowMemBench") == 0) {
                ArrowMemoryBench tempArr[] = new ArrowMemoryBench[Configuration.parallel];
                for (int i = 0; i < Configuration.parallel; i++) {
                    tempArr[i] = new ArrowMemoryBench();
                }
                for (int i = 0; i < Configuration.parallel; i++) {
                    tempArr[i].finishInit();
                    ops[i] = tempArr[i];
                }
            } else {
                throw new Exception("Illegal test name: " + Configuration.testName);
            }

            System.out.println("Allocating " + Configuration.parallel +" thread objects ");
            Thread t[] = new Thread[Configuration.parallel];
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i] = new Thread(ops[i]);
            }
            System.out.println("Test prep finished, starting the execution now ...");
            long start = System.nanoTime();
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i].start();
            }
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i].join();
            }
            long end = System.nanoTime();

            long totalBytes = 0;
            for (int i = 0; i < Configuration.parallel; i++) {
                totalBytes += ops[i].getTotalBytesProcessed();
                System.out.println("\t [" + i + "] " + ops[i].summary());
            }
            if (totalBytes == 0)
                totalBytes = BenchmarkDebugConfiguration.F100StoreSalesSizeByte;

            String bandwidthGbps = String.format("%.2f", (((double) totalBytes * 8) / (end - start)));
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("Total bytes: " + totalBytes + "(" + Utils.sizeToSizeStr2(totalBytes) + ") bandwidth " + bandwidthGbps + " Gbps");
            System.out.println("-----------------------------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
