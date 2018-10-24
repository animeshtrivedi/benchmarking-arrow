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
    std::shared_ptr<arrow::io::MemoryMappedFile> mmaped_file;
    std::cout<<"arrow file to open is : " << this->_file_name << std::endl;
    // Step 1. open the file
    arrow::Status st = arrow::io::MemoryMappedFile::Open(this->_file_name, arrow::io::FileMode::READ, &mmaped_file);
    std::cout<< "mmap file status is " << st.ok() << " zero copy support " << mmaped_file.get()->supports_zero_copy() << "\n";
    // step 2 open a reader
    st = arrow::ipc::RecordBatchFileReader::Open(mmaped_file, &this->_sptr_file_reader);
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
    for (int i = 0; i < num_batches; ++i) {
        std::cout << "attempting to load batch " << i << "\n";
        std::shared_ptr<arrow::RecordBatch> chunk;
        RETURN_NOT_OK(this->_sptr_file_reader.get()->ReadRecordBatch(i, &chunk));
    }
    return arrow::Status::OK();
}

arrow::Status ArrowReadExample::read() {
    return arrow::Status::OK();
}

