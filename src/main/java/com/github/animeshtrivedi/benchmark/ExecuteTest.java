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

import com.github.animeshtrivedi.generator.ArrowDataGenerator;
import com.github.animeshtrivedi.generator.GeneratorFactory;
import org.apache.log4j.Logger;
import scala.Tuple2;

public class ExecuteTest {
    final static Logger logger = Logger.getLogger(ExecuteTest.class);

    public ExecuteTest(){}

    public void runTest(Tuple2<String, Object>[] list){
        try {
            DataInterface[] ops = new DataInterface[Configuration.parallel];

            if (Configuration.testName.compareToIgnoreCase("datagen") == 0) {
                if(Configuration.outputDir == null){
                    throw new Exception("Data output directory is null, please give a valid name using -o");
                }
                for (int i = 0; i < Configuration.parallel; i++) {
                    HDFSWritableByteChannel w = new HDFSWritableByteChannel(Configuration.outputDir+"/part-"+i);
                    ArrowDataGenerator temp = GeneratorFactory.generator(w);
                    ops[i] = temp;
                }
            } else if (Configuration.testName.compareToIgnoreCase("ParquetToArrow") == 0) {
                for (int i = 0; i < Configuration.parallel; i++) {
                    ParquetToArrow temp = new ParquetToArrow();
                    temp.setInputOutput(list[i]._1(), Configuration.outputDir);
                    ops[i] = temp;
                }
            } else if (Configuration.testName.compareToIgnoreCase("ArrowRead") == 0) {
                for (int i = 0; i < Configuration.parallel; i++) {
                    ArrowReader temp = ArrowReader.getArrowReaderObject();
                    temp.init(list[i]._1());
                    ops[i] = temp;
                }
            } else if (Configuration.testName.compareToIgnoreCase("ArrowMemBench") == 0) {
                ArrowMemoryBench tempArr[] = new ArrowMemoryBench[Configuration.parallel];
                logger.info("...allocated " + Configuration.parallel + " ArrowMemoryBench array");
                if(Configuration.inputDir != null){
                    for (int i = 0; i < Configuration.parallel; i++) {
                        tempArr[i] = new ArrowMemoryBench(list[i]._1());
                        logger.info("...\t allocated [" + i + "] ArrowMemoryBench object for file " + list[i]._1());
                    }
                } else {
                    for (int i = 0; i < Configuration.parallel; i++) {
                        tempArr[i] = new ArrowMemoryBench();
                        logger.info("...\t allocated [" + i + "] ArrowMemoryBench object, with datagen");
                    }
                }
                logger.info("... going to wait for them to finish ");
                for (int i = 0; i < Configuration.parallel; i++) {
                    tempArr[i].finishInit();
                    logger.info("...\t finished [" + i + "] ");
                    ops[i] = tempArr[i];
                }
                RunGC.getInstance().runGC();
            } else {
                throw new Exception("Illegal test name: " + Configuration.testName);
            }

            logger.info("...allocating " + Configuration.parallel +" thread objects ");
            Thread t[] = new Thread[Configuration.parallel];
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i] = new Thread(ops[i]);
            }
            logger.info("...test prep finished, starting the execution now");
            long start = System.nanoTime();
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i].start();
            }
            for (int i = 0; i < Configuration.parallel; i++) {
                t[i].join();
            }
            long end = System.nanoTime();
            logger.info("...test ends");
            long totalBytes = 0;
            for (int i = 0; i < Configuration.parallel; i++) {
                totalBytes += ops[i].getTotalBytesProcessed();
                System.out.println("\t [" + i + "] " + ops[i].summary());
            }
            String bandwidthGbps = String.format("%.2f", (((double) totalBytes * 8) / (end - start)));
            System.out.println("-----------------------------------------------------------------------");
            System.out.println("Total bytes: " + totalBytes + "(" + Utils.sizeToSizeStr2(totalBytes) + ") bandwidth " + bandwidthGbps + " Gbps");
            System.out.println("-----------------------------------------------------------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
