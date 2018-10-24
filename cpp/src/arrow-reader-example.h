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

#ifndef BENCHMARK_ARROW_CPP_ARROWREADEREXAMPLE_H

class ArrowReadExample {
private:
    const char *_file_name;
    std::shared_ptr<arrow::Schema> _sptr_schema;
public:
    explicit ArrowReadExample(const char* filename);
    int init();
};
#define BENCHMARK_ARROW_CPP_ARROWREADEREXAMPLE_H

#endif //BENCHMARK_ARROW_CPP_ARROWREADEREXAMPLE_H
