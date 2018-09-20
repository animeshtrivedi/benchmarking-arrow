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

public class ArrowMemoryReader extends BenchmarkResults {
    private ArrowReaderDebug rx;
    private ParquetToArrow pqa;
    private MemoryIOChannel cx;
    private Thread tx;


    void setInputOutput(String inputParquetFileName) throws Exception {
        this.cx = new MemoryIOChannel();
        this.pqa = new ParquetToArrow();
        pqa.setInputOutput(inputParquetFileName, cx);
        this.tx = new Thread(pqa);
        this.tx.start();
        rx = new ArrowReaderDebug();
    }

    public void finishInit() throws Exception {
        try {
            this.tx.join();
            rx.init(cx);
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
