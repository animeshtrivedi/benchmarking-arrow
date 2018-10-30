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

#include "arrow-reader-example.h"
#include "common.h"

ArrowReadExample::ArrowReadExample(const char *file_name) {
    this->_file_name = "/home/atr/zrl/external/github/animeshtrivedi/arrow-on-crail/data/f100-ss-p15.arrow";
}

arrow::Status ArrowReadExample::init() {
    std::cout<<"arrow file to open is : " << this->_file_name << std::endl;
    // Step 1. open the file
    arrow::Status st = arrow::io::MemoryMappedFile::Open(this->_file_name, arrow::io::FileMode::READ, &this->_sptr_mmaped_file);
    std::cout<< "mmap file status is " << st.ok() << " zero copy support " << this->_sptr_mmaped_file.get()->supports_zero_copy() << "\n";
    // step 2 open a reader
    st = arrow::ipc::RecordBatchFileReader::Open(this->_sptr_mmaped_file, &this->_sptr_file_reader);
    std::cout<< "record batch file reader status is " << st.ok() << "\n";
    // step 3 find schema
    this->_sptr_schema = this->_sptr_file_reader.get()->schema();
    std::cout<< "schema is : " << this->_sptr_schema.get()->ToString() << "\n";
    // step 4 find blocks
    int num_blocks = this->_sptr_file_reader.get()->num_record_batches();
    std::cout<< "number of record batches are " << num_blocks << "\n";
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::debug_show() {
    std::cout <<"debug_show code entered \n";
    // step 1: load the batch and then index using the type
    int num_batches = this->_sptr_file_reader.get()->num_record_batches();
    for (int i = 0; i < 1; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        RETURN_NOT_OK(this->_sptr_file_reader.get()->ReadRecordBatch(i, &chunk));
        std::cout << "attempting to load batch " << i << ", contains " << chunk.get()->num_rows() << " rows and columns " << chunk.get()->num_columns() << "\n";
        RETURN_NOT_OK(this->process_batch(chunk));
    }
    return arrow::Status::OK();
}

//XXX: somehow there is something strange, if I don't return anything (which is ignored
// in the calling function I get a bad segmentation fault.
arrow::Status ArrowReadExample::process_batch(std::shared_ptr<arrow::RecordBatch> batch){
    int num_cols = batch.get()->num_columns();
    for(int i = 0; i < num_cols; i++){
        std::shared_ptr<arrow::Array> col = batch.get()->column(i);
        // we need to get the type and consume
        arrow::Type::type id = col.get()->type_id();
        switch(id){
            case arrow::Type::type::INT32 : RETURN_NOT_OK(consume_int32(col, batch.get()->num_rows())); break;
            case arrow::Type::type::INT64 : RETURN_NOT_OK(consume_int64(col, batch.get()->num_rows())); break;
            case arrow::Type::type::DOUBLE : RETURN_NOT_OK(consume_float8(col, batch.get()->num_rows())); break;
            default: std::cout << "NYI \n"; break;
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::consume_int32(std::shared_ptr<arrow::Array> col, int64_t num_rows){
    // this is an integer type values
    // how many values are there?
    std::cout << "INT32: num_rows passed is : " << num_rows << " in co length " << col.get()->length() << "\n";
    //arrow::NumericArray<arrow::Int32Type>
    // Inheritance hierarchy is
    // Array -> FlatArray -> PrimitiveArray -> NumericArray<templated>
    std::shared_ptr<arrow::Int32Array> data2 = std::dynamic_pointer_cast<arrow::Int32Array>(col);
    const int* raw_val = data2.get()->raw_values();
    const uint8_t *raw_bitmap = data2.get()->null_bitmap_data();
    for(int64_t i = 0; i < num_rows; i++){
        if(col.get()->IsValid(i)){
            std::cout << i << " is  " << data2.get()->Value(i) << "\n"; //raw_values()
            this->_total_Ints++;
        } else {
            std::cout << i << " is  NULL\n";
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::consume_int64(std::shared_ptr<arrow::Array> col, int64_t num_rows){
    // this is an integer type values
    // how many values are there?
    std::cout << "INT64: num_rows passed is : " << num_rows << " in co length " << col.get()->length() << "\n";
    //arrow::NumericArray<arrow::Int32Type>
    // Inheritance hierarchy is
    // Array -> FlatArray -> PrimitiveArray -> NumericArray<templated>
    std::shared_ptr<arrow::Int64Array> data2 = std::dynamic_pointer_cast<arrow::Int64Array>(col);
    const long* raw_val = data2.get()->raw_values();
    const uint8_t *raw_bitmap = data2.get()->null_bitmap_data();
    for(int64_t i = 0; i < num_rows; i++){
        if(col.get()->IsValid(i)){
            std::cout << i << " is  " << data2.get()->Value(i) << "\n"; //raw_values()
            this->_total_Longs++;
        } else {
            std::cout << i << " is  NULL\n";
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::consume_float8(std::shared_ptr<arrow::Array> col, int64_t num_rows) {
    // this is an integer type values
    // how many values are there?
    std::cout << "FLOAT8: num_rows passed is : " << num_rows << " in co length " << col.get()->length() << "\n";
    //arrow::NumericArray<arrow::Int32Type>
    // Inheritance hierarchy is
    // Array -> FlatArray -> PrimitiveArray -> NumericArray<templated>
    std::shared_ptr<arrow::DoubleArray> data2 = std::dynamic_pointer_cast<arrow::DoubleArray>(col);
    const double* raw_val = data2.get()->raw_values();
    const uint8_t *raw_bitmap = data2.get()->null_bitmap_data();
    for(int64_t i = 0; i < num_rows; i++){
        if(col.get()->IsValid(i)){
            std::cout << i << " is  " << data2.get()->Value(i) << "\n"; //raw_values()
            this->_total_Float8++;
        } else {
            std::cout << i << " is  NULL\n";
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::read() {
    return arrow::Status::OK();
}

