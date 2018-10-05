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

import org.apache.arrow.vector.*;

public class ArrowReaderUnsafe extends ArrowReader {

    private boolean isNull(long baseAddress, int rowIndex){
        final int byteIndex = rowIndex >> 3;
        final byte b = Platform.getByte(null, baseAddress+byteIndex);
        final int bitIndex = rowIndex & 7;
        //return Long.bitCount(b & (1L << bitIndex)) == 0;
        return (b & (1L << bitIndex)) == 0;
    }

    final public void consumeInt4(IntVector vector) {
        int valCount = vector.getValueCount();
        long valididtyAddress = vector.getValidityBuffer().memoryAddress();
        long dataAddress = vector.getDataBuffer().memoryAddress();
        for(int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                this.intCount++;
                this.checksum += Platform.getInt(null, dataAddress);
            }
            dataAddress += 4;
        }
    }
}
