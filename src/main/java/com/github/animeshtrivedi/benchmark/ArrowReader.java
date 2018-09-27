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
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
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

    public void init(String fileName) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(fileName);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream instream = fileSystem.open(path);
        FileStatus status = fileSystem.getFileStatus(path);
        this.rchannel = new HDFSSeekableByteChannel(instream, status.getLen());
        _init();
    }

    public void init(SeekableByteChannel rchannel) throws Exception {
        this.rchannel = rchannel;
        _init();
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

    private void consumeFloat4(FieldVector fv) {
        Float4Vector accessor = (Float4Vector) fv;
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!accessor.isNull(i)){
                float4Count+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void consumeFloat8(FieldVector fv) {
        Float8Vector accessor = (Float8Vector) fv;
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!accessor.isNull(i)){
                float8Count+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void consumeInt4(FieldVector fv) {
        IntVector accessor = (IntVector) fv;
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!accessor.isNull(i)){
                intCount+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void consumeBigInt(FieldVector fv) {
        BigIntVector accessor = (BigIntVector) fv;
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!accessor.isNull(i)){
                longCount+=1;
                checksum+=accessor.get(i);
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
    private void consumeBinary(FieldVector fv) {
        int length;
        VarBinaryVector accessor = (VarBinaryVector) fv;
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            if(!accessor.isNull(i)){
                binaryCount+=1;
                length = accessor.get(i).length;
                this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }

    /*
    whereas in the holder, the buffer is just referenced without being materialized
     */
    private void consumeBinaryHolder(FieldVector fv) {
        VarBinaryVector accessor = (VarBinaryVector) fv;
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        int valCount = accessor.getValueCount();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                binaryCount+=1;
                int length = holder.end  - holder.start;
                this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }

    @Override
    public void run() {
        try {
            Long s2 = System.nanoTime();
            // TODO: what is this size?
            int size = arrowBlocks.size();
            logger.debug("number of arrow block are : " + size);
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
                            consumeInt4(fv);
                            break;
                        case BIGINT:
                            consumeBigInt(fv);
                            break;
                        case FLOAT4:
                            consumeFloat4(fv);
                            break;
                        case FLOAT8:
                            consumeFloat8(fv);
                            break;
                        case VARBINARY:
                            //consumeBinary(fv);
                            consumeBinaryHolder(fv);
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
