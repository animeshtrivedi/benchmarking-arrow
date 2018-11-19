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
package com.github.animeshtrivedi.benchmark;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BufferAllocator;

public class DebugAllocatorListener implements AllocationListener {
    long allocCalled;
    long sizeSoFar;

    DebugAllocatorListener(){
        allocCalled = 0;
        sizeSoFar = 0;
    }

    @Override
    public void onAllocation(long l) {
        allocCalled++;
        sizeSoFar+=l;
        if(allocCalled < 5 )
            new Exception().printStackTrace();

        System.err.println("buffer allocation " + l + " bytes | allocCalled " + allocCalled + " sizeSoFar " + sizeSoFar);
    }

    @Override
    public boolean onFailedAllocation(long l, AllocationOutcome allocationOutcome) {
        System.err.println("Failed buffer allocation " + l + " bytes");
        return false;
    }

    @Override
    public void onChildAdded(BufferAllocator bufferAllocator, BufferAllocator bufferAllocator1) {

    }

    @Override
    public void onChildRemoved(BufferAllocator bufferAllocator, BufferAllocator bufferAllocator1) {

    }
}
