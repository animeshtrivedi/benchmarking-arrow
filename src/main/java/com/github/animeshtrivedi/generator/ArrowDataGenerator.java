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
    protected int stepping;

    protected Schema arrowSchema;
    protected VectorSchemaRoot arrowVectorSchemaRoot;
    protected ArrowFileWriter arrowFileWriter;
    protected RootAllocator ra;

    public String toString() {
        return " rows: " + rows + " columns " + columns + " stepping " + stepping;
    }

    public ArrowDataGenerator(WritableByteChannel channel){
        this.rows = Configuration.rowsPerThread;
        this.columns = Configuration.numCols;
        this.stepping = Configuration.stepping;
        // then arrow
        this.channel = channel;
        this.ra = new RootAllocator(Integer.MAX_VALUE);
    }

    protected void makeArrowSchema(String colName /*can pass "" as column name */, Types.MinorType type) throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        // generate column number of homogenous columns
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

    abstract void fillup(int count, FieldVector vector);

    public void run(){
        long rowsToGenerate = this.rows;
        List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
        try {
            this.arrowFileWriter.start();
            while (rowsToGenerate > 0) {
                int now = (int) Math.min(rowsToGenerate, this.stepping);
                this.arrowVectorSchemaRoot.setRowCount(now);
                for(int colIdx = 0; colIdx < columns; colIdx++){
                    // for all columns
                    FieldVector fv = fieldVectors.get(colIdx);
                    fv.setInitialCapacity(now);
                    fillup(now, fv);
                    fv.setValueCount(now);
                }

                // once all columns have been generated, write the batch out
                this.arrowFileWriter.writeBatch();
                // decrease the count
                rowsToGenerate-=now;
            }
            // close the writer
            this.arrowFileWriter.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
