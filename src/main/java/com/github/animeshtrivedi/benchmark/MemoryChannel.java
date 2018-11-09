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

import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;

public abstract class MemoryChannel implements WritableByteChannel, SeekableByteChannel {

    static MemoryChannel getWriteChannel(){
        //this.cx = new TimedMemoryChannel();
        if(Configuration.offHeap){
            return new OffHeapMemoryChannel();
        } else {
            return new OnHeapMemoryChannel();
        }
    }

    final public int read(ByteBuffer dst) throws IOException {
        //TODO: what is the cost of this?
        if(dst instanceof DirectBuffer)
            return readDirectUnsafe(dst);
        else
            return readSafe(dst);
    }


    @Override
    final public int write(ByteBuffer src) throws IOException {
        // there is a BUG in the Arrow contract, so we need to push all here
        int dataAvailable = src.remaining();
        while (src.hasRemaining()){
            writeOne(src);
        }
        return dataAvailable;
    }

    abstract int readDirectUnsafe(ByteBuffer dst) throws IOException;
    abstract int readSafe(ByteBuffer dst) throws IOException;
    abstract int writeOne(ByteBuffer src) throws IOException;
}
