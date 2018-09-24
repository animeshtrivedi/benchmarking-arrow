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
            if(Configuration.warmup) {
                ExecuteTest warmup = new ExecuteTest();
                warmup.runTest(list);
                logger.info("warm-up run finished, runnign GC now...");
                System.gc();
                Thread.sleep(1000);
            }
            logger.info("starting the test run");
            ExecuteTest testRun = new ExecuteTest();
            testRun.runTest(list);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
