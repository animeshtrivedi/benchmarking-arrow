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

#ifndef BENCHMARK_ARROW_CPP_INMEMORYFILE_H
#define BENCHMARK_ARROW_CPP_INMEMORYFILE_H

#include <arrow/memory_pool.h>
#include <arrow/io/file.h>
#include <arrow/status.h>
#include <arrow/ipc/reader.h>

class InMemoryFile : public arrow::io::RandomAccessFile {
public:
    InMemoryFile(){}
    virtual ~InMemoryFile(){}
    arrow::Status Close() override;
    arrow::Status GetSize(int64_t* size) override;
    arrow::Status Tell(int64_t* position) const override;
    arrow::Status Seek(int64_t position) override;
    bool supports_zero_copy() const override;
    arrow::Status Read(int64_t nbytes, int64_t* bytes_read, void* out) override;
    arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override;
    arrow::Status ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                          void* out) override;
    arrow::Status ReadAt(int64_t position, int64_t nbytes,
                          std::shared_ptr<arrow::Buffer>* out) override;
};


#endif //BENCHMARK_ARROW_CPP_INMEMORYFILE_H
