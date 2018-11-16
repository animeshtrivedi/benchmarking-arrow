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

import java.nio.ByteBuffer;

public class UnsafeBenchmark extends BenchmarkResults {

    ByteBuffer byteBuffer;
    ByteBuffer intBuffer;
    int items;

    UnsafeBenchmark(){
        items = 1 << 28;
        byteBuffer = ByteBuffer.allocateDirect(items);
        intBuffer = ByteBuffer.allocateDirect(items * 4);
        byteBuffer.clear();
        intBuffer.clear();
        System.out.println("buffers of size " + items + " allocated ");
        for(int i = 0;i < items; i++){
            byteBuffer.put(i, (byte) 0xFF);
        }
        for(int i = 0; i < items; i++){
            intBuffer.putInt(i, i);
        }

        System.out.println("buffers of size " + items + " initialized ");
    }

    private void executeByte(){
        long checkSum = 0, count = 0;
        final long address = ((sun.nio.ch.DirectBuffer) byteBuffer).address();
        final long start = System.nanoTime();
        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < items; i++) {
                checkSum += Platform.getByte(null, address + i);
                count++;
            }
        }
        final long end = System.nanoTime();
        System.out.println(" [byte] checksum " + checkSum + " count " + count + " time " + (end - start) + " COST ="  + ((double) (end-start))/count  + " ns");
        this.runtimeInNS = end - start;
    }

    private void executeInt(){
        long checkSum = 0, count = 0;
        final long address = ((sun.nio.ch.DirectBuffer) intBuffer).address();
        final int ensx = (items * 4);
        final long start = System.nanoTime();
        for(int j = 0; j < 10; j++) {
            for (int i = 0; i < ensx; i += 4) {
                checkSum += Platform.getInt(null, address + i);
                count++;
            }
        }
        final long end = System.nanoTime();
        System.out.println(" [int] checksum " + checkSum + " count " + count + " time " + (end - start)  + " COST ="  + ((double) (end-start))/count + " ns");
        this.runtimeInNS = end - start;
    }

    @Override
    public void run() {
        executeByte();
        executeInt();
    }
}
