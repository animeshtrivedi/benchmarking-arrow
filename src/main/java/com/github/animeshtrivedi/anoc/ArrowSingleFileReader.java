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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;

public class ArrowSingleFileReader extends BenchmarkResults {
    private ArrowFileReader arrowFileReader;
    private VectorSchemaRoot root;
    private List<ArrowBlock> arrowBlocks;
    private List<FieldVector> fieldVector;

    private long longCount;
    private long intCount;
    private long float4Count;
    private long float8Count;
    private long binaryCount;
    private long binarySizeCount;
    private double checksum;
    private long totalRows;
    private long runTimeInNanoSecs;

    public void init(String fileName) throws Exception {
        Configuration conf = new Configuration();
        Path path = new Path(fileName);
        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream instream = fileSystem.open(path);
        FileStatus status = fileSystem.getFileStatus(path);
        HdfsSeekableByteChannel arrowInputStream = new HdfsSeekableByteChannel(instream, status.getLen());
        this.arrowFileReader = new ArrowFileReader(new SeekableReadChannel(arrowInputStream),
                new RootAllocator(Integer.MAX_VALUE));
        this.root = arrowFileReader.getVectorSchemaRoot();
        this.arrowBlocks = arrowFileReader.getRecordBlocks();
        this.fieldVector = root.getFieldVectors();

        this.longCount = 0;
        this.intCount = 0;
        this.float4Count = 0;
        this.float8Count = 0;
        this.binaryCount = 0;
        this.binarySizeCount = 0;
        this.totalRows = 0;
        this.runTimeInNanoSecs = 0;
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

    @Override
    public void run() {
        try {
            Long s2 = System.nanoTime();
            // TODO: what is this size?
            int size = arrowBlocks.size();
            for (int i = 0; i < size; i++) {
                ArrowBlock rbBlock = arrowBlocks.get(i);
                if (!arrowFileReader.loadRecordBatch(rbBlock)) {
                    throw new IOException("Expected to read record batch");
                }
                this.totalRows += root.getRowCount();
                /* read all the fields */
                int numCols = fieldVector.size();
                for (int j = 0; i < numCols; j++) {
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
                            consumeBinary(fv);
                            break;
                        default:
                            throw new Exception("Unknown minor type: " + fv.getMinorType());
                    }
                }
            }
            long s3 = System.nanoTime();
            this.runTimeInNanoSecs = s3 - s2;
            arrowFileReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    long totalInts() {
        return this.intCount;
    }

    @Override
    long totalLongs() {
        return this.longCount;
    }

    @Override
    long totalFloat8() {
        return this.float8Count;
    }

    @Override
    long totalFloat4() {
        return this.float4Count;
    }

    @Override
    long totalBinary() {
        return this.binaryCount;
    }

    @Override
    long totalBinarySize() {
        return this.binarySizeCount;
    }

    @Override
    double getChecksum(){
        return this.checksum;
    }
}
