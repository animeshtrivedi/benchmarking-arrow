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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class HdfsSeekableByteChannel implements SeekableByteChannel {

    private FSDataInputStream instream;
    private long fileSize;
    private long truncatedSize;
    private boolean isOpen;

    public HdfsSeekableByteChannel(FSDataInputStream instream, long fileSize){
        this.instream = instream;
        this.fileSize = fileSize;
        this.truncatedSize = fileSize;
        this.isOpen = true;
    }

    @Override
    final public int read(ByteBuffer dst) throws IOException {
        return this.instream.read(dst);
    }

    @Override
    final public int write(ByteBuffer src) throws IOException {
        throw new IOException("write call on read channel");
    }

    @Override
    final public long position() throws IOException {
        return this.instream.getPos();
    }

    @Override
    final public SeekableByteChannel position(long newPosition) throws IOException {
        if(newPosition > this.truncatedSize){
            throw new IOException("Illegal seek, truncatedSize is " + this.truncatedSize +
                    " asked seek location " + newPosition +
                    " fileCapacity " + this.fileSize);
        }
        this.instream.seek(newPosition);
        return this;
    }

    @Override
    final public long size() throws IOException {
        return this.truncatedSize;
    }

    @Override
    final public SeekableByteChannel truncate(long size) throws IOException {
        this.truncatedSize = size;
        return this;
    }

    @Override
    final public boolean isOpen() {
        return isOpen;
    }

    @Override
    final public void close() throws IOException {
        this.instream.close();
        this.isOpen = false;
    }
}
