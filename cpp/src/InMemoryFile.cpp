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

#include<sys/stat.h>
#include <iostream>
#include <fstream>

#include "InMemoryFile.h"

arrow::Status InMemoryFile::GetSize(int64_t* size){
    //https://stackoverflow.com/questions/5840148/how-can-i-get-a-files-size-in-c/6039648#6039648
    struct stat stat_buf;
    int rc = stat(this->_file, &stat_buf);
    if(rc == 0){
        *size = stat_buf.st_size;
        std::cout<< __FUNCTION__ << " size of the file is " << *size << " bytes " << "\n";
        return arrow::Status::OK();
    } else {
        return arrow::Status::IOError("errno is " + errno);
    }
    //c++ way
    //std::ifstream in(this->_file, std::ifstream::ate | std::ifstream::binary);
    //*size = in.tellg();
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read,
                     void* out){
    std::cout<< __FUNCTION__ << " 1 \n";
    RETURN_NOT_OK(Seek(position));
    // now we should read
    _in->read(reinterpret_cast<char*>(out), nbytes);
    if(!_in->good()){
        std::cout <<" read failed " << _in->rdstate() << "\n";
        return arrow::Status::IOError("");
    }
    *bytes_read = nbytes;
    return arrow::Status::OK();
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes,
                     std::shared_ptr<arrow::Buffer>* out){
    std::cout<< __FUNCTION__ << "position " << position << " bytes " << nbytes << " size of the buffer " << *out << " \n";
    // logic copied from HdfsReadableFile.cc from Arrow
    std::shared_ptr<arrow::ResizableBuffer> buffer;
    RETURN_NOT_OK(AllocateResizableBuffer(_memory_pool, nbytes, &buffer));
    int64_t bytes_read = 0;
    RETURN_NOT_OK(ReadAt(position, nbytes, &bytes_read, buffer->mutable_data()));
    if (bytes_read < nbytes) {
        RETURN_NOT_OK(buffer->Resize(bytes_read));
        buffer->ZeroPadding();
    }
    *out = buffer;
    return arrow::Status::OK();
}

arrow::Status InMemoryFile::Tell(int64_t* position) const {
    std::cout<< __FUNCTION__ << "\n";
    return arrow::Status::NotImplemented("NYI");
}

arrow::Status InMemoryFile::Seek(int64_t position){
    std::cout<< __FUNCTION__ << " position " << position << "\n";
    _in->seekg(position);
    if(!_in->good()) {
        std::cout <<" seek failed " << _in->rdstate() << "\n";
        return arrow::Status::IOError("");
    }
    return arrow::Status::OK();
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
