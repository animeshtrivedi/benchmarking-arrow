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
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;

public class OffHeapMemoryChannel extends MemoryChannel {
    final static Logger logger = Logger.getLogger(OffHeapMemoryChannel.class);
    private Boolean isOpen;
    private long wSize;
    private long  rMax;
    private int arrIndex, byteArrayOffset;
    private ArrayList<ByteBuffer> dataArray;
    private final int size, shift;

    public OffHeapMemoryChannel(){
        isOpen = true;
        wSize = 0 ;
        arrIndex = 0;
        byteArrayOffset = 0;
        dataArray = new ArrayList<>();
        shift = Configuration.writeBufferSizeShift;
        size = Configuration.writeBufferSize;
        dataArray.add(ByteBuffer.allocateDirect(size));
    }

    public void fakeInit(int items){
        while(dataArray.size() < items){
            dataArray.add(ByteBuffer.allocateDirect(size));
        }
    }
    public void resetIndexes(){
        byteArrayOffset = 0;
        arrIndex = 0;
    }

    final public int readSafe(ByteBuffer dst) throws IOException {
        int originalReadTarget = dst.remaining();
        int readTarget = originalReadTarget;
        while (readTarget > 0 && arrIndex < dataArray.size()){
            int available = (size - byteArrayOffset);
            int toRead = Math.min(readTarget, available);
            ByteBuffer src = dataArray.get(arrIndex);
            src.position(byteArrayOffset);
            src.limit(byteArrayOffset + toRead);
            dst.put(src);
            byteArrayOffset+=toRead;
            readTarget-=toRead;
            if(byteArrayOffset == size){
                // new to move the index
                arrIndex++;
                byteArrayOffset=0;
            }
        }
        return originalReadTarget - readTarget;
    }

    final public int readDirectUnsafe(ByteBuffer dst) throws IOException {
        assert dst instanceof DirectBuffer;
        long dstOffset = ((DirectBuffer)dst).address() + dst.position();
        int originalReadTarget = dst.remaining();
        int readTarget = originalReadTarget;
        while (readTarget > 0 && arrIndex < dataArray.size()){
            int available = (size - byteArrayOffset);
            long toRead = Math.min(readTarget, available);
            long srcOffset = ((DirectBuffer)dataArray.get(arrIndex)).address() + byteArrayOffset;
            Platform.copyMemory(null, srcOffset, null, dstOffset, toRead);
            byteArrayOffset+=toRead;
            dstOffset+=toRead;
            readTarget-=toRead;
            if(byteArrayOffset == size){
                // new to move the index
                arrIndex++;
                byteArrayOffset=0;
            }
        }
        int numBytesRead = originalReadTarget - readTarget;
        dst.position(dst.position() + numBytesRead);
        return numBytesRead;
    }

    final int writeOne(ByteBuffer src) throws IOException {
        int dataAvailable = src.remaining();
        int oldLimit = src.limit();
        // step 1, check if we have enough space
        if((byteArrayOffset + dataAvailable) > size){
            // copy a part here and then copy later
            int toCopy = size - byteArrayOffset;
            ByteBuffer dst = dataArray.get(arrIndex);
            dst.position(byteArrayOffset);
            dst.limit(byteArrayOffset + toCopy);

            src.limit(src.position() + toCopy);
            dst.put(src);
            // add a new entry
            dataArray.add(ByteBuffer.allocateDirect(size));
            // then move indexes
            byteArrayOffset=0;
            arrIndex++;
        }

        int toCopy = Math.min(src.remaining(), size - byteArrayOffset);
        ByteBuffer dst = dataArray.get(arrIndex);
        dst.position(byteArrayOffset);
        dst.limit(byteArrayOffset + toCopy);
        src.limit(src.position() + toCopy);
        dst.put(src);
        byteArrayOffset+=toCopy;
        src.limit(oldLimit);
        this.wSize+=(dataAvailable - src.remaining());
        return dataAvailable - src.remaining();
    }

    private void show(String debug){
        System.err.println("arrIndex " + arrIndex + " offset " + byteArrayOffset + " size " + size + " shift " + shift + " | debug " + debug);
    }

    @Override
    public long position() throws IOException {
        return (arrIndex << shift) + byteArrayOffset;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        if (newPosition > rMax)
            throw new IOException(" NYI rMax: " + rMax + " newPosition " + newPosition);

        this.arrIndex = (int) (newPosition >> shift);
        this.byteArrayOffset = (int) (newPosition - (((long)this.arrIndex) << shift));
        return this;
    }

    @Override
    public long size() throws IOException {
        return wSize;
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        throw new IOException("NYI size: " + size + " wSize " + wSize);
    }

    @Override
    public boolean isOpen() {
        return isOpen;
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        rMax=wSize;
        this.arrIndex = 0;
        this.byteArrayOffset = 0;
    }
}
