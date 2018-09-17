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

public class ArrowMemoryReader extends BenchmarkResults {
    ArrowSingleFileReader rx;

    void setInputOutput(String inputParquetFileName) throws Exception {
        MemoryIOChannel cx = new MemoryIOChannel();
        ParquetToArrow pqa = new ParquetToArrow();
        pqa.setInputOutput(inputParquetFileName, cx);
        pqa.run();
        rx = new ArrowSingleFileReader();
        rx.init(cx);
    }

    public void run() {
        rx.run();
    }


    long totalInts(){
        return rx.totalInts();
    }

    long totalLongs(){
        return rx.totalLongs();
    }

    long totalFloat8(){
        return rx.totalFloat8();
    }

    long totalFloat4() {
        return rx.totalFloat4();
    }
    long totalBinary(){
        return rx.totalBinary();
    }

    long totalBinarySize() {
        return rx.totalBinarySize();
    }

    long totalRows(){
        return rx.totalRows();
    }

    public double getChecksum(){
        return rx.getChecksum();
    }

    long getRunTimeinNS() {
        return rx.getRunTimeinNS();
    }
}
