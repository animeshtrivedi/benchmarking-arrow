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
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class TimedMemoryChannel extends MemoryChannel {
    int MAX, index;
    long timestamps[];
    MemoryChannel channel;

    public TimedMemoryChannel(int items, MemoryChannel channel){
        super();
        MAX=items;
        timestamps = new long[MAX];
        index = 0;
        this.channel = channel;
    }

    public TimedMemoryChannel(MemoryChannel channel){
        this(10000, channel);
    }

    public int read(ByteBuffer dst) throws IOException {
        this.timestamps[index] = System.nanoTime();
        int ret = this.channel.read(dst);
        this.timestamps[index] = System.nanoTime() - this.timestamps[index];
        index++;
        if(index == this.timestamps.length)
            index = 0;
        return ret;
    }

    @Override
    public long position() throws IOException {
        return this.channel.position();
    }

    @Override
    public SeekableByteChannel position(long newPosition) throws IOException {
        return this.channel.position(newPosition);
    }

    @Override
    public long size() throws IOException {
        return this.channel.size();
    }

    @Override
    public SeekableByteChannel truncate(long size) throws IOException {
        return this.channel.truncate(size);
    }

    @Override
    public boolean isOpen() {
        return this.channel.isOpen();
    }

    public void close() throws IOException{
        this.channel.close();
        double average = 0.0;
        long min = Long.MAX_VALUE, max = 0;
        for(int i =0; i < index; i++){
            if(min > this.timestamps[i])
                min = this.timestamps[i];
            if(max < this.timestamps[i])
                max = this.timestamps[i];

            average+=this.timestamps[i];
            System.err.println("\t ["+i+"] " + this.timestamps[i]);
        }
        System.err.println(" average " + Utils.commaLongNumber((long)(average/index)) + " nsec, min " + Utils.commaLongNumber(min) + " max " + Utils.commaLongNumber(max));

    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return this.channel.write(src);
    }
}
