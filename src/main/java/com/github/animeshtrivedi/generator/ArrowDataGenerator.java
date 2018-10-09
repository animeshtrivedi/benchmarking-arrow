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
package com.github.animeshtrivedi.generator;

import com.github.animeshtrivedi.benchmark.Configuration;
import com.github.animeshtrivedi.benchmark.DataInterface;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.channels.WritableByteChannel;
import java.util.List;

public abstract class ArrowDataGenerator implements DataInterface {
    protected WritableByteChannel channel;
    protected long rows;
    protected int columns;
    protected String genMode;
    protected long batchCount;

    protected Schema arrowSchema;
    protected VectorSchemaRoot arrowVectorSchemaRoot;
    protected ArrowFileWriter arrowFileWriter;
    protected RootAllocator ra;

    public String toString() {
        return " rows: " + rows + " columns " + columns + " genMode " + genMode;
    }

    public ArrowDataGenerator(WritableByteChannel channel){
        this.rows = Configuration.rowsPerThread;
        this.columns = Configuration.numCols;
        // then arrow
        this.channel = channel;
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.batchCount = 0;
    }

    protected void makeArrowSchema(String colName /*can pass "" as column name */, Types.MinorType type) throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        // generate column number of homogeneous columns
        switch (type) {
            case INT :
                for(int i = 0; i < this.columns; i++){
                    childrenBuilder.add(new Field(colName+i,
                            FieldType.nullable(new ArrowType.Int(32, true)), null));
                }
                break;
            case BIGINT:
                for(int i = 0; i < this.columns; i++){
                    childrenBuilder.add(new Field(colName+i,
                            FieldType.nullable(new ArrowType.Int(64, true)), null));
                }
                break;
            case FLOAT8:
                for(int i = 0; i < this.columns; i++){
                    childrenBuilder.add(new Field(colName+i,
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                }
                break;
            case FLOAT4:
                for(int i = 0; i < this.columns; i++){
                    childrenBuilder.add(new Field(colName+i,
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                }
                break;
            case VARBINARY:
                for(int i = 0; i < this.columns; i++){
                    childrenBuilder.add(new Field(colName+i,
                            FieldType.nullable(new ArrowType.Binary()), null));
                }
                break;
            default : throw new Exception(" NYI " + type);
        }
        this.arrowSchema = new Schema(childrenBuilder.build(), null);
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot,
                provider,
                this.channel);
    }

    abstract int fillBatch(int startIndex, int endIndex, FieldVector vector);
    public int fillOne(int index, FieldVector vector){
        return this.fillBatch(index, index+1, vector);
    }

    public void runWithRows(){
        long rowsToGenerate = this.rows;
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        try {
            this.arrowFileWriter.start();
            while (rowsToGenerate > 0) {
                int now = (int) Math.min(rowsToGenerate, Configuration.arrowBlockSizeInRows);
                this.arrowVectorSchemaRoot.setRowCount(now);
                for(int colIdx = 0; colIdx < columns; colIdx++){
                    // for all columns
                    FieldVector fv = fieldVectors.get(colIdx);
                    fv.setInitialCapacity(now);
                    int nonNullrows = fillBatch(0, now, fv);
                    fv.setValueCount(nonNullrows);
                }
                // once all columns have been generated, write the batch out
                this.arrowFileWriter.writeBatch();
                this.batchCount++;
                // decrease the count
                rowsToGenerate-=now;
            }
            // close the writer
            this.arrowFileWriter.close();
            this.genMode = " with batch size set to " + Configuration.arrowBlockSizeInRows + " rows batchCount " + this.batchCount;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void runWithSize(){
        long rowsToGenerate = this.rows;
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        try {
            this.arrowFileWriter.start();
            while (rowsToGenerate > 0) {
                //batch loop
                int rowIndex = 0;
                int sizeSoFar = 0;
                while(sizeSoFar < Configuration.arrowBlockSizeInBytes && rowIndex < rowsToGenerate) {
                    //within the batch
                    sizeSoFar = 0;
                    // go over all the columns, they add one row only while keeping previous stuff
                    for (int colIdx = 0; colIdx < columns; colIdx++) {
                        // for all columns
                        FieldVector fv = fieldVectors.get(colIdx);
                        fillOne(rowIndex, fv);
                        //+1 from the index for the value count
                        fv.setValueCount(rowIndex+1);
                        // now we ask for total memory consumption of this column so far
                        sizeSoFar+=fv.getBufferSize();
                    }
                    rowIndex++;
                }
                // set the batch row index
                this.arrowVectorSchemaRoot.setRowCount(rowIndex);
                // now we write out
                this.arrowFileWriter.writeBatch();
                this.batchCount++;
                // decrease the rowsToGenerate
                rowsToGenerate-=rowIndex;
            }
            // close the writer
            this.arrowFileWriter.close();
            this.genMode = " with batch size set to " + Configuration.arrowBlockSizeInBytes + " bytes, batchCount " + this.batchCount;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void run(){
        try{
            if(Configuration.arrowBlockSizeInBytes > 0){
                runWithSize();
            } else if(Configuration.arrowBlockSizeInRows > 0){
                runWithRows();
            } else {
                throw new Exception("not a valid running configuration");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
