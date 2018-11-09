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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

// This class reads an arrow file and dumps it on the MemoryChannel
public class ArrowFileDumper implements Runnable {
    private RandomAccessFile file;
    private MemoryChannel channel;
    private ByteBuffer buffer;

    ArrowFileDumper(String arrowFileName, MemoryChannel channel){
        try {
            //TODO: Ugly hack, file: prefix comes from HDFS enumeration
            if(arrowFileName.startsWith("file:")){
                arrowFileName = arrowFileName.substring("file:".length(), arrowFileName.length());
            }
            file = new RandomAccessFile(arrowFileName, "r");
            System.out.println("setup done for " + arrowFileName + " size " + this.file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.channel = channel;
        this.buffer = ByteBuffer.allocate(Configuration.writeBufferSize);
    }

    @Override
    public void run() {
        // here we read and push it into the channel
        long readSoFar = 0;
        try {
            long fileSize = file.length();
            int toRead;
            while(readSoFar < fileSize){
                if((fileSize - readSoFar) > Integer.MAX_VALUE){
                    toRead = this.buffer.capacity();
                } else {
                    toRead = Math.min((int) (fileSize - readSoFar), this.buffer.capacity());
                }
                this.file.readFully(this.buffer.array(), 0 , toRead);
                readSoFar+=toRead;
                this.buffer.clear();
                this.buffer.limit(toRead);
                this.channel.write(this.buffer);
            }
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("Read " + readSoFar + " bytes");
    }
}
