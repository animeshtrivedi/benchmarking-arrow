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
import sun.misc.GC;

public class ArrowMemoryBench extends BenchmarkResults {
    final static Logger logger = Logger.getLogger(ArrowMemoryBench.class);
    private BenchmarkResults rx;
    private ArrowDataGenerator generator;
    private MemoryIOChannel cx;
    private Thread tx;

    public ArrowMemoryBench() throws Exception {
        this.cx = new MemoryIOChannel();
        // in this constructor we generate data on the fly to benchmark
        this.generator = GeneratorFactory.generator(this.cx);
        this.tx = new Thread(this.generator);
        this.tx.start();
    }

    public ArrowMemoryBench(String parquetFileName) throws Exception {
        this.cx = new MemoryIOChannel();
        // in this constructor we read the parquet file and hold in memory
        ParquetToArrow parquetFileReader = new ParquetToArrow();
        parquetFileReader.setInputOutput(parquetFileName, this.cx);
        this.tx = new Thread(parquetFileReader);
        this.tx.start();
    }

    public void finishInit() throws Exception {
        try {
            this.tx.join();
            if(this.generator != null){
                logger.info(this.generator.toString());
            } else {
                logger.info("parquet data reading finished");
            }
            logger.info("Running GC now...");
            System.gc();
            logger.info("...sleeping for 5 seconds");
            Thread.sleep(5000);
            if(Configuration.debug) {
                ArrowReaderDebug tmp = new ArrowReaderDebug();
                tmp.init(cx);
                this.rx = tmp;
            } else {
                ArrowReader tmp = new ArrowReader();
                tmp.init(cx);
                this.rx = tmp;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        rx.run();
    }

    public long totalInts(){
        return rx.totalInts();
    }

    public long totalLongs(){
        return rx.totalLongs();
    }

    public long totalFloat8(){
        return rx.totalFloat8();
    }

    public long totalFloat4() {
        return rx.totalFloat4();
    }
    public long totalBinary(){
        return rx.totalBinary();
    }

    public long totalBinarySize() {
        return rx.totalBinarySize();
    }

    public long totalRows(){
        return rx.totalRows();
    }

    public double getChecksum(){
        return rx.getChecksum();
    }

    public long getRunTimeinNS() {
        return rx.getRunTimeinNS();
    }
}
