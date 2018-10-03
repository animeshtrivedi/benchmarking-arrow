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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

public class ArrowReader extends BenchmarkResults {
    final static Logger logger = Logger.getLogger(ArrowReader.class);
    private ArrowFileReader arrowFileReader;
    private VectorSchemaRoot root;
    private List<ArrowBlock> arrowBlocks;
    private List<FieldVector> fieldVector;
    private SeekableByteChannel rchannel;
    private BufferAllocator allocator;
    private long timestamps[];

    protected ArrowReader(){}

    public static ArrowReader getArrowReaderObject(){
        if(com.github.animeshtrivedi.benchmark.Configuration.useHolder)
            return new ArrowHolderReader();
        else
            return new ArrowReader();
    }

    public ArrowReader init(String fileName) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(fileName);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream instream = fileSystem.open(path);
        FileStatus status = fileSystem.getFileStatus(path);
        this.rchannel = new HDFSSeekableByteChannel(instream, status.getLen());
        _init();
        return this;
    }

    public ArrowReader init(SeekableByteChannel rchannel) throws Exception {
        this.rchannel = rchannel;
        _init();
        return this;
    }

    private void _init() throws Exception {
        //this.allocator = new TracerAllocator(new DebugAllocatorListener(), Integer.MAX_VALUE);
        this.allocator = new TracerAllocator(Integer.MAX_VALUE);
        this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(this.rchannel),
                this.allocator);
        this.root = arrowFileReader.getVectorSchemaRoot();
        this.arrowBlocks = arrowFileReader.getRecordBlocks();
        this.fieldVector = root.getFieldVectors();
    }

    protected void consumeFloat4(Float4Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!vector.isNull(i)){
                float4Count+=1;
                checksum+=vector.get(i);
            }
        }
    }

    protected void consumeFloat8(Float8Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!vector.isNull(i)){
                float8Count+=1;
                checksum+=vector.get(i);
            }
        }
    }

    protected void consumeInt4(IntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!vector.isNull(i)){
                intCount+=1;
                checksum+=vector.get(i);
            }
        }
    }

    protected void consumeBigInt(BigIntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!vector.isNull(i)){
                longCount+=1;
                checksum+=vector.get(i);
            }
        }
    }

    /*
    public byte[] get(int index) {
    assert index >= 0;
    if (isSet(index) == 0) {
      throw new IllegalStateException("Value at index is null");
    }
    final int startOffset = getstartOffset(index);
    final int dataLength =
            offsetBuffer.getInt((index + 1) * OFFSET_WIDTH) - startOffset;
    final byte[] result = new byte[dataLength];
    valueBuffer.getBytes(startOffset, result, 0, dataLength);
    return result;
  }
  Call to get(i) allocates a new buffer every time.
     */
    protected void consumeBinary(VarBinaryVector vector) {
        int length;
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!vector.isNull(i)){
                binaryCount+=1;
                length = vector.get(i).length;
                this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }

    @Override
    final public void run() {
        try {
            Long s2 = System.nanoTime();
            // TODO: what is this size?
            int size = arrowBlocks.size();
            this.timestamps = new long[size];
            for (int i = 0; i < size; i++) {
                this.timestamps[i] = System.nanoTime();
                ArrowBlock rbBlock = arrowBlocks.get(i);
                if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                    throw new IOException("Expected to read record batch");
                }
                this.totalRows += root.getRowCount();
                /* read all the fields */
                int numCols = fieldVector.size();
                for (int j = 0; j < numCols; j++) {
                    FieldVector fv = fieldVector.get(j);
                    switch (fv.getMinorType()) {
                        case INT:
                            consumeInt4((IntVector) fv);
                            break;
                        case BIGINT:
                            consumeBigInt((BigIntVector) fv);
                            break;
                        case FLOAT4:
                            consumeFloat4((Float4Vector) fv);
                            break;
                        case FLOAT8:
                            consumeFloat8((Float8Vector) fv);
                            break;
                        case VARBINARY:
                            consumeBinary((VarBinaryVector) fv);
                            break;
                        default:
                            throw new Exception("Unknown minor type: " + fv.getMinorType());
                    }
                }
                this.timestamps[i] = System.nanoTime() - this.timestamps[i];
            }
            long s3 = System.nanoTime();
            this.runtimeInNS = s3 - s2;
            arrowFileReader.close();
            for (int i = 0; i < this.timestamps.length && com.github.animeshtrivedi.benchmark.Configuration.verbose; i++) {
                System.err.println("\t runstamp: " + Utils.commaLongNumber(timestamps[i]) + " nanosec | plot " + timestamps[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
