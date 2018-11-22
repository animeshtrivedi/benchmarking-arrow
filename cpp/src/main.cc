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

#include "common.h"
#include "arrow-reader-example.h"
#include "arrow-reader.h"
#include "PeakPerformance.h"
#include "CLikeRoutine.h"
#include "PeakPerformanceJava.h"

int main(int argc, char **argv) {
    std::cout << "--------------------------------------------------------------------------\n";
    std::cout << " usage: \"filename\" (which arrow file to open)] \"d\" (run in the debug mode) \n";
    std::cout << "--------------------------------------------------------------------------\n";
    // this is benchmark code
    ArrowReader *r = NULLPTR;
    if (argc > 1) {
        // use the first one as the file name
        std::cout << "Opening the file : " << argv[1] << "\n";
        r = new ArrowReader(argv[1]);
    } else {
        std::cout
                << "Opening the _DEFAULT_ file : /home/atr/zrl/external/github/animeshtrivedi/arrow-on-crail/data/f100-ss-p15.arrow" "\n";
        r = new ArrowReader("/home/atr/zrl/external/github/animeshtrivedi/arrow-on-crail/data/f100-ss-p15.arrow");
    }
    r->init();

    if(argc > 2 && strcasecmp(argv[2], "d") == 0){
        std::cout<<"Running _with_ debug \n";
        r->runWithDebug();
    } else {
        std::cout<<"Running _with_out_ debug \n";
        r->run();
    }

    std::cout << r->summary() << "\n";
    return 0;
}

#if 0

    // this is example code
#if 0
    ArrowReadExample *ex = new ArrowReadExample("/");
  std::cout << "location of the object is " << ex << "\n";
  arrow::Status s = ex->init();
  s = ex->debug_show();
  s = ex->read();
#endif


#if 0
    //PeakPerformance *r = new PeakPerformance(atol(argv[1]), atol(argv[2]));
    PeakPerformanceJava *r = new PeakPerformanceJava();
    r->init();
    if(argc > 2 && strcasecmp(argv[2], "d") == 0){
        std::cout<<"Running _with_ debug \n";
        r->runWithDebug();
    } else {
        std::cout<<"Running _with_out_ debug \n";
        r->run();
    }

    std::cout << r->summary() << "\n";
    CLikeRoutine *r = new CLikeRoutine();
  r->run();
#endif
//
//#if 0
//  long csum = r->getChecksum();
//  if( csum != 303414922182153){
//    std::cout<<"Checksum _DOES_NOT_ match " << "\n";
//  } else{
//    std::cout<<"Checksum _MATCHES_ " << "\n";
//  }
//#endif

#endif