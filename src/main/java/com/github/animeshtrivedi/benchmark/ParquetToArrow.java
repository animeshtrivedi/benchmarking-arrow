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

import com.google.common.collect.ImmutableList;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;


public class ParquetToArrow extends BenchmarkResults {
    final static Logger logger = Logger.getLogger(ParquetToArrow.class);
    // parquet objects
    protected org.apache.hadoop.conf.Configuration conf;
    protected MessageType parquetSchema;
    protected ParquetFileReader parquetFileReader;
    protected ParquetMetadata parquetFooter;
    protected ColumnReader readers[];
    protected int maxColumns;
    protected List<ColumnDescriptor> colDesc;

    // arrow objects
    protected Schema arrowSchema;
    protected VectorSchemaRoot arrowVectorSchemaRoot;
    protected ArrowFileWriter arrowFileWriter;
    protected RootAllocator ra;
    protected List<FieldVector> fieldVectors;


    // read,write channel
    protected WritableByteChannel wchannel;

    //stats
    long batchNumbers;

    protected class DumpConverter extends PrimitiveConverter {
    }

    protected class DumpGroupConverter extends GroupConverter {
        @Override
        public final Converter getConverter(int i) {
            return new DumpConverter();
        }
        @Override
        public final void start() {

        }
        @Override
        public final void end() {

        }
    }

    public ParquetToArrow(){
        this.conf = new org.apache.hadoop.conf.Configuration();
        // TODO:
        // (i) what does this mean to set it to Integer.MAX_VALUE
        // (ii) what other options are there for RootAllocator
        this.ra = new RootAllocator(Integer.MAX_VALUE);
        this.wchannel = null;
        this.batchNumbers = 0;
    }

    private Path _setParquetInput(String inputParquetFileName) throws Exception {
        Path parquetFilePath = new Path(inputParquetFileName);
        this.parquetFooter = ParquetFileReader.readFooter(conf,
                parquetFilePath,
                ParquetMetadataConverter.NO_FILTER);

        FileMetaData mdata = this.parquetFooter.getFileMetaData();
        this.parquetSchema = mdata.getSchema();
        this.parquetFileReader = new ParquetFileReader(conf,
                mdata,
                parquetFilePath,
                this.parquetFooter.getBlocks(),
                this.parquetSchema.getColumns());
        this.maxColumns = parquetSchema.getColumns().size();
        this.readers = new ColumnReader[this.maxColumns];
        this.colDesc = parquetSchema.getColumns();
        return parquetFilePath;
    }


    public void setInputOutput(String inputParquetFileName, String arrowOutputDirectory) throws Exception {
        Path p = _setParquetInput(inputParquetFileName);
        setOutputChannel(convertParquetToArrowFileName(p), arrowOutputDirectory);
        _setupArrow();
    }

    public void setInputOutput(String inputParquetFileName, WritableByteChannel wchannel) throws Exception {
        _setParquetInput(inputParquetFileName);
        this.wchannel = wchannel;
        _setupArrow();
    }

    private void _setupArrow() throws Exception {
        makeArrowSchema();
        this.arrowVectorSchemaRoot = VectorSchemaRoot.create(this.arrowSchema, this.ra);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        this.arrowFileWriter = new ArrowFileWriter(this.arrowVectorSchemaRoot,
                provider,
                this.wchannel);
        this.fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
    }

    private String convertParquetToArrowFileName(Path parquetNamePath){
        String oldsuffix = ".parquet";
        String newSuffix = ".arrow";
        String fileName = parquetNamePath.getName();
        if (!fileName.endsWith(oldsuffix)) {
            return fileName + newSuffix;
        } else {
            return fileName.substring(0, fileName.length() - oldsuffix.length()) + newSuffix;
        }
    }

    private void makeArrowSchema() throws Exception {
        ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        for(ColumnDescriptor col: this.parquetSchema.getColumns()){
            sb.setLength(0);
            String[] p = col.getPath();
            for(String px: p)
                sb.append(px);
            switch (col.getType()) {
                case INT32 :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Int(32, true)), null));
                    break;
                case INT64 :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Int(64, true)), null));
                    break;
                case INT96 :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Int(96, true)), null));
                    break;
                case DOUBLE :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null));
                    break;
                case BINARY :
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Binary()), null));
                    break;
                case FLOAT:
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null));
                    break;
                case BOOLEAN:
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.Bool()), null));
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    childrenBuilder.add(new Field(sb.toString(),
                            FieldType.nullable(new ArrowType.FixedSizeBinary(Configuration.maxByteWidth)), null));
                    break;
                default : throw new Exception(" NYI " + col.getType());
            }
        }
        this.arrowSchema = new Schema(childrenBuilder.build(), null);
        logger.debug("Arrow Schema is " + this.arrowSchema.toString());
    }

    private void setOutputChannel(String arrowFileName, String arrowOutputDirectory) throws Exception {
        // this alone should be sufficient with fully qualified path names like file://, hdfs://, or crail://
        Path arrowPath = new Path(arrowOutputDirectory);
        String arrowFullPath = arrowPath.toUri().toString() + "/" + arrowFileName;
        logger.info("Creating a file : " + arrowFullPath);
        this.wchannel = new HDFSWritableByteChannel(arrowFullPath);
    }

    private void sealAndWrite(int rows) throws IOException {
        //flush and reset
        for (int k = 0; k < this.maxColumns; k++) {
            // for all columns
            this.arrowVectorSchemaRoot.getFieldVectors().get(k).setValueCount(rows);
        }
        this.arrowVectorSchemaRoot.setRowCount(rows);
        this.arrowFileWriter.writeBatch();
        this.batchNumbers++;
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

    private int loadNextParquetBatch() throws IOException {
        int newRows = 0;
        PageReadStore pageReadStore = parquetFileReader.readNextRowGroup();
        if(pageReadStore !=null){
            ColumnReadStoreImpl colReader = new ColumnReadStoreImpl(pageReadStore, new DumpGroupConverter(),
                    this.parquetSchema, this.parquetFooter.getFileMetaData().getCreatedBy());
            if (pageReadStore.getRowCount() > Integer.MAX_VALUE)
                throw new IOException(" More than Integer.MAX_VALUE is not supported " + pageReadStore.getRowCount());
            newRows = (int) pageReadStore.getRowCount();
            List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
            //initialize all the new readers
            for (int k = 0; k < maxColumns; k++) {
                readers[k] = colReader.getColumnReader(colDesc.get(k));
            }
        }
        return newRows;
    }

    private int consumeRows(int startOffset, int rows) throws Exception {
        int bufferedSize = 0;
        //consume parquet rows and return the arrow buffer size
        for (int k = 0; k < this.maxColumns; k++) {
            //consume target rows in all columns
            ColumnDescriptor col = this.colDesc.get(k);
            switch (col.getType()) {
                case INT32:
                    writeIntColumn(readers[k],
                            col.getMaxDefinitionLevel(),
                            this.fieldVectors.get(k),
                            startOffset,
                            rows);
                    break;

                case INT64:
                    writeLongColumn(this.readers[k],
                            col.getMaxDefinitionLevel(),
                            this.fieldVectors.get(k),
                            startOffset,
                            rows);
                    break;

                case DOUBLE:
                    writeDoubleColumn(this.readers[k],
                            col.getMaxDefinitionLevel(),
                            this.fieldVectors.get(k),
                            startOffset,
                            rows);
                    break;

                case BINARY:
                    writeBinaryColumn(this.readers[k],
                            col.getMaxDefinitionLevel(),
                            this.fieldVectors.get(k),
                            startOffset,
                            rows);
                    break;

                default:
                    throw new Exception(" NYI " + col.getType());
            }
            //+1
            this.fieldVectors.get(k).setValueCount(startOffset + rows);
            bufferedSize+=this.fieldVectors.get(k).getBufferSize();
        }
        return bufferedSize;
    }

    public void run(){
        StringBuilder build = new StringBuilder();
        if(Configuration.arrowBlockSizeInBytes == -1 && Configuration.arrowBlockSizeInRows == -1){
            build.append("Running in the default parquet == arrow batch size ");
            runSameAsParquet();
        } else {
            if(Configuration.arrowBlockSizeInRows > 0){
                build.append("Running in the row count mode, with the batch count of " + Configuration.arrowBlockSizeInRows + " rows");
                runWithCount();
            } else {
                build.append("Running in the size mode, with the batch size of " + Configuration.arrowBlockSizeInBytes + " bytes");
                runWithSize();
            }
        }
        build.append(", making " + this.batchNumbers + " batches");
        logger.info(build.toString());
    }

    private void runSameAsParquet() {
        try {
            this.arrowFileWriter.start();
            long start = System.nanoTime();
            int newRows = loadNextParquetBatch();
            while(newRows > 0) {
                this.totalRows += newRows;
                // this batch of Arrow contains these many records
                consumeRows(0, newRows);
                sealAndWrite(newRows);
                newRows = loadNextParquetBatch();
            }
            this.arrowFileWriter.end();
            this.arrowFileWriter.close();
            this.runtimeInNS = System.nanoTime() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runWithSize(){
        try {
            int writeIndex = 0;
            int bufferedSize = 0;
            this.arrowFileWriter.start();
            long start = System.nanoTime();
            int newRows = loadNextParquetBatch();
            while (newRows > 0){
                // now we have the column loaded
                this.totalRows += newRows;
                //consume all the new rows
                while (newRows > 0) {
                    bufferedSize = consumeRows(writeIndex, 1);
                    if (bufferedSize >= Configuration.arrowBlockSizeInBytes) {
                        sealAndWrite(writeIndex);
                        writeIndex = 0;
                    }
                    writeIndex += 1;
                    newRows -= 1;
                }
                // we made sure that we are going to reload
                newRows = loadNextParquetBatch();
            }
            if(writeIndex > 0) {
                //flush the end segment
                sealAndWrite(writeIndex);
            }
            this.arrowFileWriter.end();
            this.arrowFileWriter.close();
            this.runtimeInNS = System.nanoTime() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runWithCount(){
        try {
            int writeIndex = 0;
            this.arrowFileWriter.start();
            long start = System.nanoTime();
            int newRows = loadNextParquetBatch();
            while (newRows > 0) {
                this.totalRows += newRows;
                while (newRows > 0) {
                    int target = Math.min(Configuration.arrowBlockSizeInRows - writeIndex, newRows);
                    consumeRows(writeIndex, target); //ignore the return value
                    //update wrote
                    writeIndex += target;
                    if (writeIndex == Configuration.arrowBlockSizeInRows) {
                        sealAndWrite(writeIndex);
                        //reset
                        writeIndex = 0;
                    }
                    newRows -= target;
                }
                // we made sure that we are going to reload
                newRows = loadNextParquetBatch();
            }
            if(writeIndex  > 0) {
                //flush the end segment
                sealAndWrite(writeIndex);
            }
            this.arrowFileWriter.end();
            this.arrowFileWriter.close();
            this.runtimeInNS = System.nanoTime() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
