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
package com.github.animeshtrivedi.arrow;

import com.github.animeshtrivedi.benchmark.*;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

public class ArrowIntReader extends BenchmarkResults {
    private SeekableByteChannel channel;
    private ByteBuffer valueBuffer;
    private ByteBuffer validBuffer;

    private ArrowFileReader arrowFileReader;
    private VectorSchemaRoot root;
    private List<ArrowBlock> arrowBlocks;
    private List<FieldVector> fieldVector;
    private BufferAllocator allocator;
    private int items, bitmapSize;
    ByteBuffer buf;

    public ArrowIntReader(MemoryChannel channel){
        this.channel = channel;
        // now we do some sanity checks and setup the read pattern
        try {
            _init();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void _init() throws Exception {
        this.allocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(this.channel),
                this.allocator);
        this.root = arrowFileReader.getVectorSchemaRoot();
        this.arrowBlocks = arrowFileReader.getRecordBlocks();
        this.fieldVector = root.getFieldVectors();
        System.err.println("There are " + arrowBlocks.size() + " blocks");
        if(Configuration.arrowBlockSizeInRows == -1){
            throw new Exception("set the arrowBlockSizeInRows is needed ");
        }
        this.items = Configuration.arrowBlockSizeInRows; //only mode in which it will work
        // the bitmap size is rows/8 + aligned size
        this.bitmapSize = items/8;
        if(items % 8 != 0) {
            bitmapSize++;
        }
        // align the bitmap size to 8 bytes
        if(bitmapSize % 8 != 0){
            bitmapSize+=(8 - (bitmapSize % 8 ));
        }
        if(Configuration.offHeap)
            this.buf = ByteBuffer.allocateDirect((items * Integer.BYTES) + bitmapSize);
        else
            this.buf = ByteBuffer.allocate((items * Integer.BYTES) + bitmapSize);
    }

    private boolean _verifyContent(String expectedContent, long position) throws IOException {
        long currentPosition = this.channel.position();
        this.channel.position(position);
        ByteBuffer content = ByteBuffer.allocate(expectedContent.length());
        this.channel.read(content);
        this.channel.position(currentPosition);
        return (new String(content.array()).compareTo(expectedContent) == 0);
    }

    private boolean verifyHeader() throws IOException {
        return _verifyContent("ARROW1", 0);
    }

    private boolean verifyFooter() throws IOException {
        return _verifyContent("ARROW1", this.channel.size() - 6);
    }

    public static int bytesToInt(byte[] bytes) {
        return bytesToInt(bytes, 0);
    }

    public static int bytesToInt(byte[] bytes, int offset) {
        return ((bytes[offset + 3] & 255) << 24) +
                ((bytes[offset + 2] & 255) << 16) +
                ((bytes[offset + 1] & 255) << 8) +
                ((bytes[offset] & 255) << 0);
    }

    public void matchMyReading(){
        try {
            System.err.println(" Size of the file is : " + this.channel.size());
            System.err.println(" Verify header " + verifyHeader());
            System.err.println(" Verify footer " + verifyFooter());
            ByteBuffer intx = ByteBuffer.allocate(Integer.BYTES);
            this.channel.position(this.channel.size() - 10); // 6 + 4
            this.channel.read(intx);
            System.err.println(" Size of the ArrowFooter is " + ArrowIntReader.bytesToInt(intx.array()));
            // 192 bytes, matches
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        long s = System.nanoTime();
        if(Configuration.offHeap) {
            runOffHeap();
        } else {
            runOnHeap();
        }
        this.runtimeInNS = System.nanoTime() - s;
    }

    public void runOnHeap(){
        long avg_load = 0, avg_consume = 0;
        try {
            // reading the file block by block
            for (int i = 0; i < arrowBlocks.size(); i++) {
                long s = System.nanoTime();
                //System.err.println("\t " + JavaUtils.toString(arrowBlocks.get(i)));
                ArrowBlock block = arrowBlocks.get(i);
                this.channel.position(block.getOffset() + block.getMetadataLength());
                buf.clear();
                // clear the buffer and reuse it again and again
                this.channel.read(buf);
                long s2 = System.nanoTime();
                consumeIntBatchOnHeap(buf, bitmapSize, (int) block.getBodyLength());
                long s3= System.nanoTime();
                avg_load+=(s2 - s);
                avg_consume+=(s3 -s2);
            }
            avg_load/=arrowBlocks.size();
            avg_consume/=arrowBlocks.size();
            System.err.println(" AVG load = " + avg_load + " ns, consume = " + avg_consume + " \n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runOffHeap(){
        try {
            // reading the file block by block
            for (int i = 0; i < arrowBlocks.size(); i++) {
                //System.err.println("\t " + JavaUtils.toString(arrowBlocks.get(i)));
                ArrowBlock block = arrowBlocks.get(i);
                this.channel.position(block.getOffset() + block.getMetadataLength());
                buf.clear();
                this.channel.read(buf);
                //consumeIntBatchDirect(buf, bitmapSize, (int) block.getBodyLength());
                consumeIntBatchDirectNullCheck(buf, bitmapSize, (int) block.getBodyLength());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void runOffHeapTrace(){
        long t0,t1,t2,t3;
        try {
            // reading the file block by block
            for (int i = 0; i < arrowBlocks.size(); i++) {
                t0 = System.nanoTime();
                //System.err.println("\t " + JavaUtils.toString(arrowBlocks.get(i)));
                ArrowBlock block = arrowBlocks.get(i);
                this.channel.position(block.getOffset() + block.getMetadataLength());
                t1 = System.nanoTime();
                buf.clear();
                int rex = this.channel.read(buf);
                t2  = System.nanoTime();
                //consumeIntBatchDirect(buf, bitmapSize, (int) block.getBodyLength());
                consumeIntBatchDirectNullCheck(buf, bitmapSize, (int) block.getBodyLength());
                t3 = System.nanoTime();
                System.err.println(i + "setup " + Utils.commaLongNumber(t1-t0) + " reading " + Utils.commaLongNumber(t2-t1) + " ns, consume  " + (Utils.commaLongNumber(t3-t2)) + " rex " + rex + " buffer status " + buf);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void consumeIntBatchOnHeap(ByteBuffer buf, int bitmapSize, int bodySize) throws Exception{
        // now the buffer contains [BITfield + Values] in this sequence, lets consume them
        for(int k = bitmapSize; k < bodySize; k+=Integer.BYTES){
            this.intCount++;
            //this.checksum+=buf.getInt(k);
            // TODO: there is a bit of mismatch between the LE of Arrow and BE of the JVM
            // TODO: need to find out more
            this.checksum+=bytesToInt(buf.array(), k);
            //System.err.println("\t" + buf.getInt(k));//bytesToInt(all.array(), k));
        }
    }

    private void consumeIntBatchDirect(ByteBuffer buf, int bitmapSize, int bodySize) throws Exception{
        long address = ((DirectBuffer) buf).address();
        for(int k = bitmapSize; k < bodySize; k+=Integer.BYTES){
            this.intCount++;
            //this.checksum+=buf.getInt(k);
            //this.checksum+=bytesToInt(buf.array(), k);
            //System.err.println(Platform.getInt(null, address+k));
            this.checksum+=Platform.getInt(null, address+k);
        }
    }

    private boolean isNull(long baseAddress, int rowIndex){
        final int byteIndex = rowIndex >> 3;
        final byte b = Platform.getByte(null, baseAddress+byteIndex);
        final int bitIndex = rowIndex & 7;
        //return Long.bitCount(b & (1L << bitIndex)) == 0;
        return (b & (1L << bitIndex)) == 0;
    }

    private int isSet(long baseAddress, int rowIndex){
        final int byteIndex = rowIndex >> 3;
        final byte b = Platform.getByte(null, baseAddress+byteIndex);
        final int bitIndex = rowIndex & 7;
        return Long.bitCount(b & (1L << bitIndex));
    }

    private int isSetLong(long baseAddress, int rowIndex){
        final int byteIndex = rowIndex >> 3;
        final long b = Platform.getLong(null, baseAddress+byteIndex);
        final int bitIndex = rowIndex & 7;
        return Long.bitCount(b & (1L << bitIndex));
    }


    private void consumeIntBatchDirectNullCheck(ByteBuffer buf, int bitmapSize, int bodySize) throws Exception{
        long address = ((DirectBuffer) buf).address();
        long k = address + bitmapSize;
        for(int r = 0; r < Configuration.arrowBlockSizeInRows; r++){
            if(!isNull(address, r)){
                this.intCount++;
                //this.checksum+=buf.getInt(k);
                //this.checksum+=bytesToInt(buf.array(), k);
                //System.err.println(Platform.getInt(null, address+k));
                this.checksum+=Platform.getInt(null, k);
            }
            k+=Integer.BYTES;
        }
    }


    private void consumeIntBatchDirectNullCheckWithAlloc(ByteBuffer bufx, int bitmapSize, int bodySize) throws Exception{
        ArrowBuf arrowBuffer = this.allocator.buffer(bodySize);
        // mark the area we are about to use !
        arrowBuffer.setIndex(0, bodySize);
        //buffer.retain(this.allocator);
        ByteBuffer nioBuf = arrowBuffer.nioBuffer();
        this.channel.read(nioBuf);
        long address = arrowBuffer.memoryAddress();
        //System.err.println(JavaUtils.toHexString(address));
        long k = address + bitmapSize;
        for(int r = 0; r < Configuration.arrowBlockSizeInRows; r++){
            if(!isNull(address, r)){
                this.intCount++;
                //this.checksum+=buf.getInt(k);
                //this.checksum+=bytesToInt(buf.array(), k);
                //System.err.println(Platform.getInt(null, address+k));
                this.checksum+=Platform.getInt(null, k);
            }
            k+=Integer.BYTES;
        }
        arrowBuffer.release();
        arrowBuffer = null; //gc
    }
}
