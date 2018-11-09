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
#include <fcntl.h>

#include "common.h"
#include "InMemoryFile.h"
#include "Debug.h"


#include <sys/mman.h>
#include <assert.h>

arrow::Status InMemoryFile::GetSize(int64_t* size){
    //https://stackoverflow.com/questions/5840148/how-can-i-get-a-files-size-in-c/6039648#6039648
    struct stat stat_buf;
    int rc = stat(this->_file, &stat_buf);
    if(rc == 0){
        *size = stat_buf.st_size;
        _debug(__FUNCTION__ << " size of the file is " << *size << " bytes ");
        return arrow::Status::OK();
    } else {
        return arrow::Status::IOError("");
    }
    //c++ way
    //std::ifstream in(this->_file, std::ifstream::ate | std::ifstream::binary);
    //*size = in.tellg();
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out){
    _debug(__FUNCTION__ << "\n");
    RETURN_NOT_OK(Seek(position));
    memcpy(out, buffer + this->_read_ptr, nbytes);
    *bytes_read = nbytes;
    return arrow::Status::OK();
}

arrow::Status InMemoryFile::ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<arrow::Buffer>* out){
    _debug(__FUNCTION__ << "position " << position << " bytes " << nbytes << " size of the buffer " << *out << " \n");
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
    _debug(__FUNCTION__ << " position " << position << "\n");
    this->_read_ptr = position;
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

int InMemoryFile::setupMemory() {
    int64_t size;
    this->GetSize(&size);
    int fd = open(_file, O_RDONLY, 0);
    assert(fd != -1);
    //Execute mmap with MAP_POPULATE to get all pages in memory
    buffer = (char*) mmap(NULL, size, PROT_READ, MAP_SHARED | MAP_POPULATE, fd, 0);
    assert(mmappedData != MAP_FAILED);
    _info(_file << " with size " << size << " mapped at " << (void*) buffer << "\n");
    this->_read_ptr = 0;
    return 0;
}