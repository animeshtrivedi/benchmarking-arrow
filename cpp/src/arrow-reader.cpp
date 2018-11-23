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
#include <arrow/array.h>

static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

ArrowReader::ArrowReader(const char* filename){
    this->_file_name = filename;
}

int ArrowReader::init() {
    //choose one or other
    if(true){
#ifdef USE_MMAP
        std::cout<<"Using the _mmap_ MemoryMappedFile interface for " << this->_file_name << "\n";
        std::shared_ptr<arrow::io::MemoryMappedFile> file;
        arrow::Status st = arrow::io::MemoryMappedFile::Open(this->_file_name,
                                                             arrow::io::FileMode::READ,
                                                             &file);
#else
        std::cout<<"Using the ReadableFile interface for " << this->_file_name << "\n";
        std::shared_ptr<arrow::io::ReadableFile> file;
        arrow::Status st = arrow::io::ReadableFile::Open(this->_file_name, &file);
#endif
        this->_sptr_file = file;

    } else {
        std::cout<<"Using the InMemory (my implementation) interface for " << this->_file_name << "\n";
        this->_sptr_file = std::make_shared<InMemoryFile>(this->_file_name);
    }
    // step 2 open a reader
    arrow::Status st = arrow::ipc::RecordBatchFileReader::Open(this->_sptr_file,
                                                               &this->_sptr_file_reader);
    // step 3 find schema
    this->_sptr_schema = this->_sptr_file_reader.get()->schema();
    // step 4 find blocks
    int num_blocks = this->_sptr_file_reader.get()->num_record_batches();
    return 0;
}


int ArrowReader::runWithDebug() {
#if 0
    int num_batches = this->_sptr_file_reader.get()->num_record_batches();
    //int64_t *load_time = new int64_t[num_batches];
    //int64_t *process_time = new int64_t[num_batches];
    int64_t avg_load=0, avg_process=0;
    auto start = std::chrono::high_resolution_clock::now();
    // step 1: load the batch and then index using the type
    for (int i = 0; i < num_batches; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        auto s1 = std::chrono::high_resolution_clock::now();
        this->_sptr_file_reader.get()->ReadRecordBatch(i, &chunk);
        auto s2 = std::chrono::high_resolution_clock::now();
        this->process_batch(chunk);
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
#endif
    return 0;
}

#ifdef DO_CONST
int ArrowReader::run() {
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> localPtr = this->_sptr_file_reader;
    long checkSum = 0, intCount = 0;
    auto start = std::chrono::high_resolution_clock::now();
    // step 1: load the batch and then index using the type
    int num_batches = localPtr.get()->num_record_batches();
    for (int i = 0; i < num_batches; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        localPtr.get()->ReadRecordBatch(i, &chunk);
        this->process_batch(chunk, intCount, checkSum);
    }
    //TODO: clean up ?
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    this->_checksum = checkSum;
    this->_total_Ints = intCount;
    return 0;
}

int ArrowReader::process_batch(std::shared_ptr<arrow::RecordBatch> batch,  long &intCount, long &checkSum) const {
    int num_cols = batch.get()->num_columns(), ret = -1;
    for(int i = 0; i < num_cols; i++){
        std::shared_ptr<arrow::Array> col = batch.get()->column(i);
        // we need to get the type and consume
        arrow::Type::type id = col.get()->type_id();
        switch(id){
            case arrow::Type::type::INT32 : ret = consume_int32(col, intCount, checkSum); break;
            default: std::cout << "NYI \n"; break;
        }
    }
    return ret;
}
#if 1
int ArrowReader::consume_int32(std::shared_ptr<arrow::Array> col, long &intCount, long &checkSum) const {
    const int* const raw_val = std::dynamic_pointer_cast<arrow::Int32Array>(col).get()->raw_values();
    const unsigned long items = col.get()->length();
    // get the raw data here
    const std::shared_ptr<arrow::ArrayData> data = col.get()->data();
    // get the raw bitmap
    const uint8_t *bitmap = col.get()->null_bitmap_data();
    // use the local variables instead of using the class
    unsigned long intsx = 0, checksumx = 0;
    if(bitmap == NULLPTR){
        // fast path
        for (unsigned long i = 0; i < items; i++){
            checksumx+=raw_val[i];
        }
        intsx = items;
    } else {
        // slow path
        for (unsigned long i = 0, j = data->offset; i < items; i++, j++) {
            //if ((bitmap[j >> 3] & (1 << (j & 0x07))) != 0){
            //if ((bitmap[j >> 3] & kBitmask[j & 0x07]) != 0){
            if ((bitmap[j / 8] & kBitmask[j % 8]) != 0){ // gives 14.1 Gbps
            //if ((bitmap[j / 8] & kBitmask[(j & 0x07)]) != 0){ // gives 18.08 Gbps
                intsx++;
                checksumx += raw_val[i];
            }
        }
    }
    intCount+=intsx;
    checkSum+=checksumx;
    return 0;
}
#else

int ArrowReader::consume_int32(std::shared_ptr<arrow::Array> col, long &intCount, long &checkSum) const {
    const int* const raw_val = std::dynamic_pointer_cast<arrow::Int32Array>(col).get()->raw_values();
    const int64_t items = col.get()->length();
    // use the local variables instead of using the class
    long intsx = 0, checksumx = 0;
    // slow path
    for (int64_t i = 0; i < items; i++) {
        if (col->IsValid(i)){
            intsx++;
            checksumx += raw_val[i];
        }
    }
    intCount+=intsx;
    checkSum+=checksumx;
    return 0;
}
#endif

#else
int ArrowReader::run() {
    const std::shared_ptr<arrow::ipc::RecordBatchFileReader> localPtr = this->_sptr_file_reader;
    auto start = std::chrono::high_resolution_clock::now();
    // step 1: load the batch and then index using the type
    int num_batches = localPtr.get()->num_record_batches();
    for (int i = 0; i < num_batches; ++i) {
        std::shared_ptr<arrow::RecordBatch> chunk;
        localPtr.get()->ReadRecordBatch(i, &chunk);
        this->process_batch(chunk);
    }
    //TODO: clean up ?
    auto end = std::chrono::high_resolution_clock::now();
    this->_runtime_in_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    return 0;
}

int ArrowReader::process_batch(std::shared_ptr<arrow::RecordBatch> batch) {
    int num_cols = batch.get()->num_columns(), ret = -1;
    for(int i = 0; i < num_cols; i++){
        std::shared_ptr<arrow::Array> col = batch.get()->column(i);
        // we need to get the type and consume
        arrow::Type::type id = col.get()->type_id();
        switch(id){
            case arrow::Type::type::INT32 : ret = consume_int32(col); break;
            case arrow::Type::type::INT64 : ret = consume_int64(col); break;
            case arrow::Type::type::DOUBLE : ret = consume_float8(col); break;
            default: std::cout << "NYI \n"; break;
        }
    }
    return ret;
}

int ArrowReader::consume_int32(std::shared_ptr<arrow::Array> col){
    const int* const raw_val = std::dynamic_pointer_cast<arrow::Int32Array>(col).get()->raw_values();
    const int64_t items = col.get()->length();
    // get the raw data here
    const std::shared_ptr<arrow::ArrayData> data = col.get()->data();
    // get the raw bitmap
    const uint8_t *bitmap = col.get()->null_bitmap_data();
    // use the local variables instead of using the class
    long ints = 0, checksum = 0;
    if(bitmap == NULLPTR){
        // fast path
        for (int64_t i = 0; i < items; i++){
            checksum+=raw_val[i];
        }
        ints = items;
    } else {
        // slow path
        for (int64_t i = 0, j = data->offset; i < items; i++, j++) {
            if ((bitmap[j >> 3] & kBitmask[j & 0x07]) != 0){
                ints++;
                checksum += raw_val[i];
            }
        }
    }
    this->_total_Ints+=ints;
    this->_checksum+=checksum;
    return 0;
}
#endif

int ArrowReader::consume_int64(std::shared_ptr<arrow::Array> col){
    std::shared_ptr<arrow::Int64Array> data2 = std::dynamic_pointer_cast<arrow::Int64Array>(col);
    const long* raw_val = data2.get()->raw_values();
    for(int64_t i = 0; i < col.get()->length(); i++){
        if(col.get()->IsValid(i)){
            this->_total_Longs++;
            this->_checksum+= raw_val[i];
        }
    }
    return 0;
}

int ArrowReader::consume_float8(std::shared_ptr<arrow::Array> col){
    std::shared_ptr<arrow::DoubleArray> data2 = std::dynamic_pointer_cast<arrow::DoubleArray>(col);
    const double * raw_val = data2.get()->raw_values();
    for(int64_t i = 0; i < col.get()->length(); i++){
        if(col.get()->IsValid(i)){
            this->_total_Float8++;
            this->_checksum+=raw_val[i];
        }
    }
    return 0;
}