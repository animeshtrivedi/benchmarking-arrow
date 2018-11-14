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

#include "arrow-reader.h"
#include "common.h"
#include "InMemoryFile.h"

#include <iostream>
#include <chrono>


ArrowReader::ArrowReader(const char* filename){
    this->_file_name = filename;
}

arrow::Status ArrowReader::init() {
    // Step 1. open the file - mmap
    arrow::Status st = arrow::io::MemoryMappedFile::Open(this->_file_name,
            arrow::io::FileMode::READ,
            &this->_sptr_mmaped_filex);
    // or in-memory buffer
    //std::shared_ptr<arrow::io::RandomAccessFile> in_memory_file(new InMemoryFile(this->_file_name));
    std::shared_ptr<InMemoryFile> in_memory_file = std::make_shared<InMemoryFile>(this->_file_name);

    //choose one or other
    if(!true){
        this->_sptr_file = this->_sptr_mmaped_filex;
    } else {
        this->_sptr_file = in_memory_file;
    }
    // step 2 open a reader
    st = arrow::ipc::RecordBatchFileReader::Open(this->_sptr_file,
            &this->_sptr_file_reader);
    // step 3 find schema
    this->_sptr_schema = this->_sptr_file_reader.get()->schema();
    // step 4 find blocks
    int num_blocks = this->_sptr_file_reader.get()->num_record_batches();
    return arrow::Status::OK();
}

arrow::Status ArrowReader::run() {
    auto start = std::chrono::high_resolution_clock::now();
    // step 1: load the batch and then index using the type
    int num_batches = this->_sptr_file_reader.get()->num_record_batches();
    for (int i = 0; i < num_batches; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        RETURN_NOT_OK(this->_sptr_file_reader.get()->ReadRecordBatch(i, &chunk));
        RETURN_NOT_OK(this->process_batch(chunk));
    }
    //TODO: clean up ?
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return arrow::Status::OK();
}

arrow::Status ArrowReader::runWithDebug() {
    int num_batches = this->_sptr_file_reader.get()->num_record_batches();
    //int64_t *load_time = new int64_t[num_batches];
    //int64_t *process_time = new int64_t[num_batches];
    int64_t avg_load=0, avg_process=0;
    auto start = std::chrono::high_resolution_clock::now();
    // step 1: load the batch and then index using the type
    for (int i = 0; i < num_batches; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        auto s1 = std::chrono::high_resolution_clock::now();
        RETURN_NOT_OK(this->_sptr_file_reader.get()->ReadRecordBatch(i, &chunk));
        auto s2 = std::chrono::high_resolution_clock::now();
        RETURN_NOT_OK(this->process_batch(chunk));
        auto s3 = std::chrono::high_resolution_clock::now();

        //load_time[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(s2 - s1).count();
        //process_time[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(s3 - s2).count();
        avg_load+=std::chrono::duration_cast<std::chrono::nanoseconds>(s2 - s1).count(); //load_time[i];
        avg_process+=std::chrono::duration_cast<std::chrono::nanoseconds>(s3 - s2).count();//process_time[i];
    }
    //TODO: clean up ?
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    //now print these details:
    std::cout << " load " << avg_load/num_batches << " nsec, consume " << avg_process/num_batches << " nsec \n";
    return arrow::Status::OK();
}

arrow::Status  ArrowReader::process_batch(std::shared_ptr<arrow::RecordBatch> batch){
    int num_cols = batch.get()->num_columns();
    for(int i = 0; i < num_cols; i++){
        std::shared_ptr<arrow::Array> col = batch.get()->column(i);
        // we need to get the type and consume
        arrow::Type::type id = col.get()->type_id();
        switch(id){
            case arrow::Type::type::INT32 : RETURN_NOT_OK(consume_int32(col)); break;
            case arrow::Type::type::INT64 : RETURN_NOT_OK(consume_int64(col)); break;
            case arrow::Type::type::DOUBLE : RETURN_NOT_OK(consume_float8(col)); break;
            default: std::cout << "NYI \n"; break;
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReader::consume_int32(std::shared_ptr<arrow::Array> col){
    std::shared_ptr<arrow::Int32Array> data2 = std::dynamic_pointer_cast<arrow::Int32Array>(col);
    const int* raw_val = data2.get()->raw_values();
    for(int64_t i = 0; i < col.get()->length(); i++){
        // for all valid values, the isValid is optimized to return immediately by checking if the bitmap is NULL
        if(col.get()->IsValid(i)){
            this->_total_Ints++;
            this->_checksum+= raw_val[i];
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReader::consume_int64(std::shared_ptr<arrow::Array> col){
    std::shared_ptr<arrow::Int64Array> data2 = std::dynamic_pointer_cast<arrow::Int64Array>(col);
    const long* raw_val = data2.get()->raw_values();
    for(int64_t i = 0; i < col.get()->length(); i++){
        if(col.get()->IsValid(i)){
            this->_total_Longs++;
            this->_checksum+= raw_val[i];
        }
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReader::consume_float8(std::shared_ptr<arrow::Array> col){
    std::shared_ptr<arrow::DoubleArray> data2 = std::dynamic_pointer_cast<arrow::DoubleArray>(col);
    const double * raw_val = data2.get()->raw_values();
    for(int64_t i = 0; i < col.get()->length(); i++){
        if(col.get()->IsValid(i)){
            this->_total_Float8++;
            this->_checksum+=raw_val[i];
        }
    }
    return arrow::Status::OK();
}