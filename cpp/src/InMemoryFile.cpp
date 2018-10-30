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

#include <iostream>

#include "InMemoryFile.h"

arrow::Status InMemoryFile::GetSize(int64_t* size){
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                     void* out){
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes,
                     std::shared_ptr<arrow::Buffer>* out){
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::Tell(int64_t* position) const {
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::Seek(int64_t position){
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::Close(){
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");    
}

bool InMemoryFile::supports_zero_copy() const {
    std::cout<< __FUNCTION__ << "\n";
    return false;
}

arrow::Status InMemoryFile::Read(int64_t nbytes, int64_t *bytes_read, void *out) {
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");    
}

arrow::Status InMemoryFile::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer> *out) {
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}
