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

#include <arrow/array.h>
#include <arrow/memory_pool.h>
#include <arrow/type.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/io/file.h>
#include <arrow/test-util.h>

#include "arrow-writer-example.h"

ArrowWriterExample::ArrowWriterExample() {
    auto memory_pool = arrow::default_memory_pool();
    std::cout << "__cplusplus version is : " << __cplusplus << std::endl;
    std::cout << " address of memory pool is " << memory_pool << std::endl;

    std::shared_ptr<arrow::Int32Type> dt0(new arrow::Int32Type());
    auto type = arrow::int32();

    auto f0 = arrow::field("f0", type);
    std::vector<std::shared_ptr<arrow::Field>> fields = {f0};
    auto schema = std::make_shared<arrow::Schema>(fields);
    int batch_length = 5;
    arrow::Int32Builder builder(arrow::int32(), memory_pool);
    builder.Resize(batch_length);
    //first one is null
    builder.AppendNull();
    for (int i = 1; i < batch_length; i++){
        builder.Append(i);
    }
    std::shared_ptr<arrow::Array> array;
    builder.Finish(&array);

    std::cout << "allocated a new schema at : " << schema.get() << std::endl;

    // this creates a memory mapped file
    //std::shared_ptr<ArrowReadExample::io::MemoryMappedFile> file;
    //ArrowReadExample::Status s = ArrowReadExample::io::MemoryMappedFile::Create("./arr.bin", 1024, &file);

    std::shared_ptr<arrow::io::OutputStream> file;
    arrow::Status s = arrow::io::FileOutputStream::Open("./arr.bin", false, &file);

    std::cout << "memory mapped file status is : " << s.ok() << std::endl;

    std::shared_ptr<arrow::ipc::RecordBatchWriter> file_writer;

    auto p = arrow::ipc::RecordBatchFileWriter::Open(file.get(), schema, &file_writer);
    std::cout << "file writer object is at " << p << std::endl;
    auto batch = arrow::RecordBatch::Make(schema, batch_length * 2, {array});

    file_writer.get()->WriteRecordBatch(*batch.get(), true);

    file_writer.get()->Close();
    file.get()->Close();
}