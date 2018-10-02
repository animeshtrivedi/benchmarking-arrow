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

import java.nio.channels.WritableByteChannel;
import java.util.List;


public class ParquetToArrow extends BenchmarkResults {
    final static Logger logger = Logger.getLogger(ParquetToArrow.class);
    // parquet objects
    protected org.apache.hadoop.conf.Configuration conf;
    protected MessageType parquetSchema;
    protected ParquetFileReader parquetFileReader;
    protected ParquetMetadata parquetFooter;

    // arrow objects
    protected Schema arrowSchema;
    protected VectorSchemaRoot arrowVectorSchemaRoot;
    protected ArrowFileWriter arrowFileWriter;
    protected RootAllocator ra;

    // read,write channel
    protected WritableByteChannel wchannel;

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

    public void run() {
        try {
            long start = System.nanoTime();
            PageReadStore pageReadStore = null;
            List<ColumnDescriptor> colDesc = parquetSchema.getColumns();
            List<FieldVector> fieldVectors = this.arrowVectorSchemaRoot.getFieldVectors();
            int size = colDesc.size();
            DumpGroupConverter conv = new DumpGroupConverter();
            this.arrowFileWriter.start();
            pageReadStore = parquetFileReader.readNextRowGroup();
            while (pageReadStore != null) {
                ColumnReadStoreImpl colReader = new ColumnReadStoreImpl(pageReadStore, conv,
                        this.parquetSchema, this.parquetFooter.getFileMetaData().getCreatedBy());
                if (pageReadStore.getRowCount() > Integer.MAX_VALUE)
                    throw new Exception(" More than Integer.MAX_VALUE is not supported " + pageReadStore.getRowCount());
                int rows = (int) pageReadStore.getRowCount();
                this.totalRows += rows;
                // this batch of Arrow contains these many records
                this.arrowVectorSchemaRoot.setRowCount(rows);
                int i = 0;
                while (i < size) {
                    ColumnDescriptor col = colDesc.get(i);
                    switch (col.getType()) {
                        case INT32:
                            writeIntColumn(colReader.getColumnReader(col),
                                    col.getMaxDefinitionLevel(),
                                    fieldVectors.get(i),
                                    rows);
                            break;

                        case INT64:
                            writeLongColumn(colReader.getColumnReader(col),
                                    col.getMaxDefinitionLevel(),
                                    fieldVectors.get(i),
                                    rows);
                            break;

                        case DOUBLE:
                            writeDoubleColumn(colReader.getColumnReader(col),
                                    col.getMaxDefinitionLevel(),
                                    fieldVectors.get(i),
                                    rows);
                            break;

                        case BINARY:
                            writeBinaryColumn(colReader.getColumnReader(col),
                                    col.getMaxDefinitionLevel(),
                                    fieldVectors.get(i),
                                    rows);
                            break;

                        default:
                            throw new Exception(" NYI " + col.getType());
                    }
                    i += 1;
                }
                pageReadStore = parquetFileReader.readNextRowGroup();
                this.arrowFileWriter.writeBatch();
            }
            this.arrowFileWriter.end();
            this.arrowFileWriter.close();
            this.runtimeInNS = System.nanoTime() - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeIntColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        IntVector intVector = (IntVector) fieldVector;
        intVector.setInitialCapacity(rows);
        intVector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                intVector.setSafe(i, 1, creader.getInteger());
                this.intCount++;
            } else {
                intVector.setNull(i);
            }
            creader.consume();
        }
        intVector.setValueCount(rows);
    }

    private void writeLongColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        BigIntVector bigIntVector = (BigIntVector) fieldVector;
        bigIntVector.setInitialCapacity(rows);
        bigIntVector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                bigIntVector.setSafe(i, 1, creader.getLong());
                this.longCount++;
            } else {
                bigIntVector.setNull(i);
            }
            creader.consume();
        }
        bigIntVector.setValueCount(rows);
    }

    private void writeDoubleColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        Float8Vector float8Vector  = (Float8Vector) fieldVector;
        float8Vector.setInitialCapacity((int) rows);
        float8Vector.allocateNew();

        for(int i = 0; i < rows; i++) {
            if(creader.getCurrentDefinitionLevel() == dmax){
                float8Vector.setSafe(i, 1, creader.getDouble());
                this.float8Count++;
            } else {
                float8Vector.setNull(i);
            }
            creader.consume();
        }
        float8Vector.setValueCount(rows);
    }

    private void writeBinaryColumn(ColumnReader creader, int dmax, FieldVector fieldVector, int rows) throws Exception {
        VarBinaryVector varBinaryVector  = (VarBinaryVector) fieldVector;
        varBinaryVector.setInitialCapacity((int) rows);
        varBinaryVector.allocateNew();

        for(int i = 0; i < rows; i++) {
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
        varBinaryVector.setValueCount(rows);
    }
}
