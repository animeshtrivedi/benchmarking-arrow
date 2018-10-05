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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;

public class ArrowReaderHolder extends ArrowReader {
    private NullableFloat4Holder f4Holder;
    private NullableFloat8Holder f8Holder;
    private NullableIntHolder intHolder;
    private NullableBigIntHolder longHolder;
    private NullableVarBinaryHolder binHolder;
    // The cost of checksum calculation seem to be around 400-500 Mbps for store_sales table for each thread.
    // when checksum enabled we got 4.8-5.0 Gbps and when not, 5.4-5.6 Gbps. Since, the aim of the benchamark
    // is to materialize value, which we have inside the holder object, we can leave the checksum calculation here.

    protected ArrowReaderHolder() {
        super();
        this.f4Holder = new NullableFloat4Holder();
        this.f8Holder = new NullableFloat8Holder();
        this.intHolder = new NullableIntHolder();
        this.longHolder =  new NullableBigIntHolder();
        this.binHolder = new NullableVarBinaryHolder();
    }

    final protected void consumeFloat4(Float4Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, f4Holder);
            if(f4Holder.isSet == 1){
                float4Count+=1;
                checksum+=f4Holder.value;
            }
        }
    }

    final protected void consumeFloat8(Float8Vector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, f8Holder);
            if(f8Holder.isSet == 1){
                float8Count+=1;
                checksum+=f8Holder.value;
            }
        }
    }

    private int isSet(ArrowBuf buf, int rowIndex) {
        final int byteIndex = rowIndex >> 3;
        final byte b = (byte) ((byte) byteIndex & 0xFF); //buf.getByte(byteIndex);
        final int bitIndex = rowIndex & 7;
        return Long.bitCount(b & (1L << bitIndex));
    }

    private int isSet2(ArrowBuf buf, int rowIndex) {
        final int byteIndex = rowIndex >> 3;
        return buf.getByte(byteIndex);
    }

    final protected void _consumeInt4(IntVector vector) {
        int valCount = vector.getValueCount();
        ArrowBuf D_buf = vector.getDataBuffer();
        ArrowBuf V_buf = vector.getValidityBuffer();
        System.err.println("\t data buffer " + JavaUtils.toString(D_buf) +
                " validaity buffer " + JavaUtils.toString(V_buf));
        System.err.println(JavaUtils.toStringContent(V_buf, 0, 32));

        for(int i = 0; i < valCount; i++){
            isSet2(V_buf, i);
            D_buf.getInt(i);
            //vector.isSet(i);
            //vector.get(i, intHolder);
//            if(intHolder.isSet == 1){
//                intCount+=1;
//                //checksum+=intHolder.value;
//            }
        }
        intCount+=valCount;
    }

    final protected void consumeInt4(IntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++) {
            vector.get(i, intHolder);
            if (intHolder.isSet == 1) {
                intCount += 1;
                checksum+=intHolder.value;
            }
        }
    }

    final protected void consumeBigInt(BigIntVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, longHolder);
            if(longHolder.isSet == 1){
                longCount+=1;
                checksum+=longHolder.value;
            }
        }
    }

    final protected void consumeBinary(VarBinaryVector vector) {
        int valCount = vector.getValueCount();
        for(int i = 0; i < valCount; i++){
            vector.get(i, binHolder);
            if(binHolder.isSet == 1){
                binaryCount+=1;
                int length = binHolder.end  - binHolder.start;
                //this.checksum+=length;
                this.binarySizeCount+=length;
            }
        }
    }
}
