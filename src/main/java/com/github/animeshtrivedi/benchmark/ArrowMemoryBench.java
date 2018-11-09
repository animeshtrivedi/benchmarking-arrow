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

import com.github.animeshtrivedi.arrow.ArrowIntReader;
import com.github.animeshtrivedi.generator.ArrowDataGenerator;
import com.github.animeshtrivedi.generator.GeneratorFactory;
import org.apache.log4j.Logger;

public class ArrowMemoryBench extends BenchmarkResults {
    final static Logger logger = Logger.getLogger(ArrowMemoryBench.class);
    private BenchmarkResults rx;
    private ArrowDataGenerator generator;
    private MemoryChannel cx;
    private Thread tx;

    private void _start(){
        // when we are generating data or converting a parquet files, we need a write
        // channel
        this.cx = MemoryChannel.getWriteChannel();
    }
    private void _end(){
        // we start the processing thread here
        this.tx.start();
    }

    public ArrowMemoryBench() throws Exception {
        _start();
        if(Configuration.arrowBlockSizeInBytes == -1 && Configuration.arrowBlockSizeInRows == -1){
            //none of them are set, we pick one by default that is - based on the rows
            logger.warn("*** No batch size provided, setting the rows as 1000");
            Configuration.arrowBlockSizeInRows = 1000;
        }
        // in this constructor we generate data on the fly to benchmark
        this.generator = GeneratorFactory.generator(this.cx);
        this.tx = new Thread(this.generator);
        _end();
    }

    public ArrowMemoryBench(String fileName) throws Exception {
        _start();
        if(Configuration.inputFileType == InputType.PARQUET) {
            // in this constructor we read the parquet file and hold in memory
            ParquetToArrow parquetFileReader = new ParquetToArrow();
            parquetFileReader.setInputOutput(fileName, this.cx);
            this.tx = new Thread(parquetFileReader);
        } else {
            // we are using a arrow file type
            this.tx = new Thread(new ArrowFileDumper(fileName, this.cx));
        }
        _end();
    }

    public void finishInit() throws Exception {
        try {
            this.tx.join();
            if(this.generator != null){
                logger.info(this.generator.toString());
            } else {
                logger.info("file data reading finished");
            }
            //TODO: somehow this is the key to move the performance form ~100 Gbps to 150+ Gbps on 16 cores
            // I don't know running GC here has a different effect than running in the start and end of
            // ExecuteTest framework. Also, here it runs synchronously.
            // For now the problem is gone with large enough young generation and G1GC - if we get time we
            // might return to it.
            //RunGC.getInstance().runGC();
            if(Configuration.xcode)
                this.rx  = new ArrowIntReader(this.cx);
            else
                this.rx  = ArrowReader.getArrowReaderObject().init(this.cx);

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
