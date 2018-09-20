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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

// the holder variant deliver close to 29.x Gbps, whereas the normal one goes to 39.x Gbps
public class ArrowReaderDebug extends BenchmarkResults {
    private ArrowFileReader arrowFileReader;
    private VectorSchemaRoot root;
    private List<ArrowBlock> arrowBlocks;
    private List<FieldVector> fieldVector;
    private SeekableByteChannel rchannel;

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
        this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(this.rchannel),
                new RootAllocator(Integer.MAX_VALUE));
        // this will call ensureInitialized the first time you call it
        this.root = arrowFileReader.getVectorSchemaRoot();

        this.arrowBlocks = arrowFileReader.getRecordBlocks();
        this.fieldVector = root.getFieldVectors();
    }

    public void run2() {
        try {
            Long s2 = System.nanoTime();
            // TODO: what is this size?
            int size = arrowBlocks.size();
            System.err.println("Number of arrow block are : " + size + " who sets these?");
            // this seems to come from parquet block size ~128MB
            for (int i = 0; i < size; i++) {
                ArrowBlock rbBlock = arrowBlocks.get(i);
                if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                    throw new IOException("Expected to read record batch");
                }
                this.totalRows += root.getRowCount();
                System.err.println("\t arrow row count is " + root.getRowCount());
                /* read all the fields */
                int numCols = fieldVector.size();
                for (int j = 0; j < numCols; j++) {
                    FieldVector fv = fieldVector.get(j);
                    switch (fv.getMinorType()) {
                        case INT:
                            holderConsumeInt42(fv);
                            break;
                        case BIGINT:
                            holderConsumeBigInt2(fv);
                            break;
                        case FLOAT4:
                            holderConsumeFloat42(fv);
                            break;
                        case FLOAT8:
                            holderConsumeFloat82(fv);
                            break;
                        case VARBINARY:
                            holderConsumeBinary2(fv);
                            break;
                        default:
                            throw new Exception("Unknown minor type: " + fv.getMinorType());
                    }
                }
            }
            long s3 = System.nanoTime();
            this.runtimeInNS = s3 - s2;
            arrowFileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            Long s2 = System.nanoTime();
            mode2();
            long s3 = System.nanoTime();
            this.runtimeInNS = s3 - s2;
            System.err.println("\t -> bytes read " + this.arrowFileReader.bytesRead());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void mode1() {
        try {
            // when looping like this, this only loads one batch and w/o setting any row count
            while(!arrowFileReader.loadNextBatch()){
                this.totalRows += root.getRowCount();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void mode2(){
        int size = arrowBlocks.size();
        for (int i = 0; i < size; i++) {
            ArrowBlock rbBlock = arrowBlocks.get(i);
            try {
                if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                    throw new IOException("Expected to read record batch");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.totalRows += root.getRowCount();
            System.err.println("\t arrow row count is " + root.getRowCount());
        }
    }

    private void holderConsumeFloat4(FieldVector fv) {
        Float4Vector accessor = (Float4Vector) fv;
        int valCount = accessor.getValueCount();
        NullableFloat4Holder holder = new NullableFloat4Holder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                float4Count+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void holderConsumeFloat8(FieldVector fv) {
        Float8Vector accessor = (Float8Vector) fv;
        int valCount = accessor.getValueCount();
        NullableFloat8Holder holder = new NullableFloat8Holder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                float8Count+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void holderConsumeInt4(FieldVector fv) {
        IntVector accessor = (IntVector) fv;
        int valCount = accessor.getValueCount();
        NullableIntHolder holder = new NullableIntHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                intCount+=1;
                checksum+=holder.value;
            }
        }
    }

    private void holderConsumeBigInt(FieldVector fv) {
        BigIntVector accessor = (BigIntVector) fv;
        int valCount = accessor.getValueCount();
        NullableBigIntHolder holder = new NullableBigIntHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                longCount+=1;
                checksum+=accessor.get(i);
            }
        }
    }

    private void holderConsumeBinary(FieldVector fv) {
        int length;
        VarBinaryVector accessor = (VarBinaryVector) fv;
        int valCount = accessor.getValueCount();
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
            if(holder.isSet == 1){
                binaryCount+=1;
                length = holder.buffer.capacity();
                this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }

    private void holderConsumeFloat42(FieldVector fv) {
        Float4Vector accessor = (Float4Vector) fv;
        int valCount = accessor.getValueCount();
        NullableFloat4Holder holder = new NullableFloat4Holder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
        }
    }

    private void holderConsumeFloat82(FieldVector fv) {
        Float8Vector accessor = (Float8Vector) fv;
        int valCount = accessor.getValueCount();
        NullableFloat8Holder holder = new NullableFloat8Holder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
        }
    }

    private void holderConsumeInt42(FieldVector fv) {
        IntVector accessor = (IntVector) fv;
        int valCount = accessor.getValueCount();
        NullableIntHolder holder = new NullableIntHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
        }
    }

    private void holderConsumeBigInt2(FieldVector fv) {
        BigIntVector accessor = (BigIntVector) fv;
        int valCount = accessor.getValueCount();
        NullableBigIntHolder holder = new NullableBigIntHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
        }
    }

    private void holderConsumeBinary2(FieldVector fv) {
        int length;
        VarBinaryVector accessor = (VarBinaryVector) fv;
        int valCount = accessor.getValueCount();
        NullableVarBinaryHolder holder = new NullableVarBinaryHolder();
        for(int i = 0; i < valCount; i++){
            accessor.get(i, holder);
        }
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
}