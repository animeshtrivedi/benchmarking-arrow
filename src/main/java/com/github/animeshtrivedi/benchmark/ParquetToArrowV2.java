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

import org.apache.arrow.vector.*;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;

import java.io.IOException;
import java.util.List;

public class ParquetToArrowV2 extends ParquetToArrow {
    // the main difference in the implementation is to enable setting the batch size

    private void sealAndWrite(int rows, int maxColumns) throws IOException {
        //flush and reset
        for (int k = 0; k < maxColumns; k++) {
            // for all columns
            this.arrowVectorSchemaRoot.getFieldVectors().get(k).setValueCount(rows);
        }
        this.arrowVectorSchemaRoot.setRowCount(rows);
        this.arrowFileWriter.writeBatch();
    }

    public void run(){
        try {
            int wroteSoFar = 0;
            long start = System.nanoTime();
            PageReadStore pageReadStore = null;
            List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
            List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
            int maxColumns = colDesc.size();
            this.arrowFileWriter.start();
            pageReadStore = parquetFileReader.readNextRowGroup();
            while (pageReadStore != null) {
                ColumnReadStoreImpl colReader = new ColumnReadStoreImpl(pageReadStore, new DumpGroupConverter(),
                        this.parquetSchema, this.parquetFooter.getFileMetaData().getCreatedBy());
                ColumnReader []readers = new ColumnReader[maxColumns];
                //initialize all the readers
                for (int k = 0; k < maxColumns; k++) {
                    readers[k] = colReader.getColumnReader(colDesc.get(k));
                }
                if (pageReadStore.getRowCount() > Integer.MAX_VALUE)
                    throw new Exception(" More than Integer.MAX_VALUE is not supported " + pageReadStore.getRowCount());
                int newRows = (int) pageReadStore.getRowCount();
                // now we have the column loaded
                this.totalRows += newRows;
                while (newRows > 0) {
                    int target = Math.min(Configuration.arrowBlockSizeInRows - wroteSoFar, newRows);
                    // we can push as many as target rows in all columns
                    for (int k = 0; k < maxColumns; k++) {
                        //consume target rows in all columns
                        ColumnDescriptor col = colDesc.get(k);
                        switch (col.getType()) {
                            case INT32:
                                writeIntColumn(readers[k],
                                        col.getMaxDefinitionLevel(),
                                        fieldVectors.get(k),
                                        wroteSoFar,
                                        target);
                                break;

                            case INT64:
                                writeLongColumn(readers[k],
                                        col.getMaxDefinitionLevel(),
                                        fieldVectors.get(k),
                                        wroteSoFar,
                                        target);
                                break;

                            case DOUBLE:
                                writeDoubleColumn(readers[k],
                                        col.getMaxDefinitionLevel(),
                                        fieldVectors.get(k),
                                        wroteSoFar,
                                        target);
                                break;

                            case BINARY:
                                writeBinaryColumn(readers[k],
                                        col.getMaxDefinitionLevel(),
                                        fieldVectors.get(k),
                                        wroteSoFar,
                                        target);
                                break;

                            default:
                                throw new Exception(" NYI " + col.getType());
                        }
                    }
                    //update wrote
                    wroteSoFar += target;
                    if (wroteSoFar == Configuration.arrowBlockSizeInRows) {
                        sealAndWrite(wroteSoFar, maxColumns);
                        //reset
                        wroteSoFar = 0;
                    }
                    newRows -= target;
                }
                // we made sure that we are going to reload
                pageReadStore = parquetFileReader.readNextRowGroup();
            }

            if(wroteSoFar > 0) {
                //flush the end segment
                sealAndWrite(wroteSoFar, maxColumns);
            }
            this.arrowFileWriter.end();
            this.arrowFileWriter.close();
            this.runtimeInNS = System.nanoTime() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeIntColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int startIndex, int rows) throws Exception {
        IntVector intVector = (IntVector) fieldVector;
        for(int i = startIndex; i < (startIndex+rows); i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                intVector.setSafe(i, 1, creader.getInteger());
                this.intCount++;
            } else {
                intVector.setNull(i);
            }
            creader.consume();
        }
    }


    private void writeLongColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int startIndex, int rows) throws Exception {
        BigIntVector bigIntVector = (BigIntVector) fieldVector;
        for(int i = startIndex; i < (startIndex+rows); i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                bigIntVector.setSafe(i, 1, creader.getLong());
                this.longCount++;
            } else {
                bigIntVector.setNull(i);
            }
            creader.consume();
        }
    }

    private void writeDoubleColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int startIndex, int rows) throws Exception {
        Float8Vector float8Vector  = (Float8Vector) fieldVector;
        for(int i = startIndex; i < (startIndex+rows); i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                float8Vector.setSafe(i, 1, creader.getDouble());
                this.float8Count++;
            } else {
                float8Vector.setNull(i);
            }
            creader.consume();
        }
    }

    private void writeBinaryColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int startIndex, int rows) throws Exception {
        VarBinaryVector varBinaryVector  = (VarBinaryVector) fieldVector;
        for(int i = startIndex; i < (startIndex+rows); i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                byte[] data = creader.getBinary().getBytes();
                varBinaryVector.setIndexDefined(i);
                varBinaryVector.setValueLengthSafe(i, data.length);
                varBinaryVector.setSafe(i, data);
                this.binaryCount++;
                this.binarySizeCount+=data.length;
            } else {
                varBinaryVector.setNull(i);
            }
            creader.consume();
        }
    }
}
