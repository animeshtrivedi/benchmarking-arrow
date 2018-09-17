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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;

public class MemoryIOChannel implements WritableByteChannel, SeekableByteChannel {

    private Boolean isOpen;
    private long wSize;
    private long  rMax, rBaseOffset;
    private int rIndex, currByteBufferOffset;
    ArrayList<ByteBuffer> dataArray;

    public MemoryIOChannel(){
        isOpen = true;
        wSize = 0 ;
        rBaseOffset = 0;
        rIndex = 0;
        currByteBufferOffset = 0;
        dataArray = new ArrayList<>();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        ByteBuffer buf = dataArray.get(rIndex);
        int readBytesBB = buf.remaining();
        if(readBytesBB == 0 && rBaseOffset < rMax){
            if((rIndex + 1) == dataArray.size()){
                // end of the stream
                return 0;
            } else{
                rBaseOffset+=buf.capacity();
                currByteBufferOffset = 0;
                rIndex++;

            }
        }
        int toRead = Math.min(dst.remaining(), readBytesBB);
        // a byte buffer ready to read
        dst.put(buf.array(), currByteBufferOffset, toRead);
        currByteBufferOffset+=toRead;
        buf.position(currByteBufferOffset);
        return toRead;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        ByteBuffer item = ByteBuffer.allocate(remaining);
        item.put(src);
        item.clear();
        dataArray.add(item);
        // this should be remaining from the original
        wSize+=item.capacity();
        rMax=wSize;
        return remaining;
    }

    @Override
    public long position() throws IOException {
        return rBaseOffset + currByteBufferOffset;
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        throw new IOException(" NYI rMax: " + rMax + " newPosition " + newPosition);
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
    }
}
