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

// The idea of this class is to emulate how Arrow reads
// a file
public class PeakPerformance extends BenchmarkResults {
    final public static int MAX_INT_ITEMS = 1 << 28;
    final private long items;
    final private int loop;
    final private ByteBuffer bitmapBuffer;
    final private ByteBuffer intValueBuffer;

    PeakPerformance() throws Exception {
        this(100000000, true, 1000000, 100);
    }

    PeakPerformance(int items, boolean doNulls, int steps, int loop) throws Exception {
        if(items > MAX_INT_ITEMS){
            throw new Exception("items cannot be more than the max ");
        }
        this.loop = loop;
        this.items = (long) items;
        int intSize = items << 2;
        int bitmapSize = items >> 3;
        bitmapBuffer = ByteBuffer.allocateDirect(bitmapSize);
        intValueBuffer = ByteBuffer.allocateDirect(intSize);
        bitmapBuffer.clear();
        intValueBuffer.clear();
        System.out.println(items + " items = buffers of int size " + intSize + " bitmap size " + bitmapSize + " allocated ");
        for(int i = 0;i < bitmapSize; i++){
            bitmapBuffer.put(i, (byte) 0xFF);
        }
        // mark doNull
        for(int i = 0; doNulls && i < items; i+=steps){
            bitmapBuffer.put((i>>3), (byte) 0xEF);
        }

        for(int i = 0; i < items; i++){
            intValueBuffer.putInt(i<<2, i);
        }
        System.out.println("initialization done");
    }

    public void runNoRoll() {
        long checkSum = 0, count = 0;
        final long bitmapAddress = ((sun.nio.ch.DirectBuffer) bitmapBuffer).address();
        final long valueAddress = ((sun.nio.ch.DirectBuffer) intValueBuffer).address();
        //final byte[] map = {1, 2, 4, 8, 16, 32, 64, (byte) 128};
        int loopCount = 0;
        long intCount=0, runningCheckSum=0;

        final long start = System.nanoTime();
        while (loopCount < loop) {
            for (long i = 0; i < items; i++) {
                if((Platform.getByte(null, bitmapAddress + (i >> 3L)) & (1L << (i & 7L))) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
            }
            loopCount++;
        }
        final long end = System.nanoTime();
        this.runtimeInNS = end - start;
        this.checksum+= runningCheckSum;
        this.intCount+= intCount;
    }

    public void runRoll() {
        long checkSum = 0, count = 0;
        final long bitmapAddress = ((sun.nio.ch.DirectBuffer) bitmapBuffer).address();
        final long valueAddress = ((sun.nio.ch.DirectBuffer) intValueBuffer).address();
        //final byte[] map = {1, 2, 4, 8, 16, 32, 64, (byte) 128};
        int loopCount = 0;
        long intCount=0, runningCheckSum=0;

        final long start = System.nanoTime();
        while (loopCount < loop) {
            for (long i = 0; i < items;) {
                byte bx = Platform.getByte(null, bitmapAddress + (i >> 3L));
                if((bx & 1) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 2) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 4) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 8) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 16) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 32) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 64) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
                if((bx & 128) != 0) {
                    intCount++;
                    runningCheckSum += Platform.getInt(null, valueAddress + (i << 2));
                }
                i++;
            }
            loopCount++;
        }
        final long end = System.nanoTime();
        this.runtimeInNS = end - start;
        this.checksum+= runningCheckSum;
        this.intCount+= intCount;
    }

    public void run(){
        runNoRoll();
    }
}
