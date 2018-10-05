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
        long valididtyAddress = vector.getValidityBufferAddress();
        long dataAddress = vector.getDataBufferAddress();
        for(int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                //if (vector.isSet(i) == 1) {
                this.intCount++;
                this.checksum += Platform.getInt(null, dataAddress);
            }
            dataAddress += IntVector.TYPE_WIDTH;
        }
    }

    protected void consumeBigInt(BigIntVector vector) {
        int valCount = vector.getValueCount();
        long valididtyAddress = vector.getValidityBufferAddress();
        long dataAddress = vector.getDataBufferAddress();
        for(int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                this.longCount++;
                this.checksum += Platform.getLong(null, dataAddress);
            }
            dataAddress += BigIntVector.TYPE_WIDTH;
        }
    }

    protected void consumeFloat4(Float4Vector vector) {
        int valCount = vector.getValueCount();
        long valididtyAddress = vector.getValidityBufferAddress();
        long dataAddress = vector.getDataBufferAddress();
        for(int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                this.float4Count++;
                this.checksum += Platform.getFloat(null, dataAddress);
            }
            dataAddress += Float4Vector.TYPE_WIDTH;
        }
    }

    protected void consumeFloat8(Float8Vector vector) {
        int valCount = vector.getValueCount();
        long valididtyAddress = vector.getValidityBufferAddress();
        long dataAddress = vector.getDataBufferAddress();
        for (int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                this.float8Count++;
                this.checksum += Platform.getDouble(null, dataAddress);
            }
            dataAddress += Float8Vector.TYPE_WIDTH;
        }
    }

    protected void consumeBinary(VarBinaryVector vector) {
        //TODO: this is not tested yet
        int valCount = vector.getValueCount();
        long valididtyAddress = vector.getValidityBufferAddress();
        long dataAddress = vector.getDataBufferAddress();
        long offsetAddress = vector.getOffsetBufferAddress();
        for (int i = 0; i < valCount; i++) {
            if (!isNull(valididtyAddress, i)) {
                int start = Platform.getInt(null, offsetAddress);
                int length = Platform.getInt(null, offsetAddress + BaseVariableWidthVector.OFFSET_WIDTH) - start;
                this.binaryCount++;
                this.checksum += length;
                this.binarySizeCount+=length;
                //get binary play load from the data address
                //this.valueBuffer.getBytes(start, byte[], 0, dataLength);
            }
            offsetAddress+=(BaseVariableWidthVector.OFFSET_WIDTH);
        }
    }

}
