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
import org.apache.arrow.vector.ipc.message.ArrowBlock;

public class JavaUtils {
    public static String toString(ArrowBlock rbBlock){
        StringBuilder builder = new StringBuilder();
        builder.append(" ArrowBlock offset " + rbBlock.getOffset() +
                " body " + rbBlock.getBodyLength() +
                " metadata " + rbBlock.getMetadataLength());
        return builder.toString();
    }

    public static String toHexString(long address){
        return Long.toHexString(address);
    }

    public static String toString(ArrowBuf buf){
        StringBuilder builder = new StringBuilder();
        builder.append("ArrowBuf address: " + Long.toHexString(buf.memoryAddress()) +
        " capacity: " + buf.capacity());
        return builder.toString();
    }


    public static String toStringContent(ArrowBuf buf, int offset, int length){
        StringBuilder builder = new StringBuilder();
        int upto = Math.min(offset+length, buf.capacity());
        for(int i = offset; i < upto; i++){
            if(i % 16 == 0)
                builder.append("\n");
            builder.append(String.format(" %02X ", buf.getByte(i)));
        }
        return builder.toString();
    }

    public static boolean allZero(ArrowBuf buf, int offset, int length){
        StringBuilder builder = new StringBuilder();
        boolean allZero = true;
        for(int i = offset; i < (offset+length); i++) {
            if ((buf.getByte(i) & 0xFF) != 0)
                allZero = false;
        }
        return allZero;
    }

    public static boolean allOne(ArrowBuf buf, int offset, int length){
        StringBuilder builder = new StringBuilder();
        boolean allOne = true;
        for(int i = offset; i < (offset+length); i++) {
            if ((buf.getByte(i) | 0xFF) != 0xFF)
                allOne = false;
        }
        return allOne;
    }

}
