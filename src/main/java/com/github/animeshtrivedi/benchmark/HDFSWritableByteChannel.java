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

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class HDFSWritableByteChannel implements WritableByteChannel {

    private FSDataOutputStream outStream;
    private Boolean isOpen;
    private byte[] tempBuffer;

    public HDFSWritableByteChannel(FSDataOutputStream outStream){
        this.outStream = outStream;
        this.isOpen = true;
        this.tempBuffer = new byte[BenchmarkConfiguration.writeBufferSize];
    }

    private int writeDirectBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        int soFar = 0;
        while(soFar < remaining){
            int toPush = Math.min(remaining - soFar, this.tempBuffer.length);
            // this will move the position index
            src.get(this.tempBuffer, 0, toPush);
            // i have no way of knowing how much can i push at HDFS
            this.outStream.write(this.tempBuffer, 0, toPush);
            soFar+=toPush;
        }
        return remaining;
    }

    private int writeHeapBuffer(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        // get the heap buffer directly and copy
        this.outStream.write(src.array(), src.position(), remaining);
        src.position(src.position() + remaining);
        return remaining;
    }

    @Override
    final public int write(ByteBuffer src) throws IOException {
        if(src.isDirect()){
            return writeDirectBuffer(src);
        } else {
            return writeHeapBuffer(src);
        }
    }

    @Override
    final public boolean isOpen() {
        return this.isOpen;
    }

    @Override
    final public void close() throws IOException {
        // flushes the client buffer
        this.outStream.hflush();
        // to the disk
        this.outStream.hsync();
        this.outStream.close();
        this.isOpen = false;
    }
}