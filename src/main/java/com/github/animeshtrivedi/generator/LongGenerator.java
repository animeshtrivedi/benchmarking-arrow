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
package com.github.animeshtrivedi.generator;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;

import java.nio.channels.WritableByteChannel;

public class LongGenerator extends ArrowDataGenerator {
    int totalLongs;

    public LongGenerator(WritableByteChannel channel) {
        super(channel);
        try {
            super.makeArrowSchema("", Types.MinorType.BIGINT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.totalLongs = 0;
    }

    @Override
    int fillBatch(int startIndex, int endIndex, FieldVector vector){
        BigIntVector intVector = (BigIntVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            intVector.setSafe(i, 1, (long) i);
        }
        // for debugging mark one NULL
        //intVector.setSafe(endIndex/2, 0, 0);
        this.totalLongs+=(endIndex - startIndex);
        //but we generated all longs
        return (endIndex - startIndex);
    }

    public String toString() {
        return "LongGenerator" + super.toString();
    }

    @Override
    public long totalInts() {
        return this.totalLongs;
    }

    @Override
    public long totalLongs() {
        return 0;
    }

    @Override
    public long totalFloat8() {
        return 0;
    }

    @Override
    public long totalFloat4() {
        return 0;
    }

    @Override
    public long totalBinary() {
        return 0;
    }

    @Override
    public long totalBinarySize() {
        return 0;
    }

    @Override
    public long totalRows() {
        return 0;
    }

    @Override
    public double getChecksum() {
        return 0;
    }

    @Override
    public long getRunTimeinNS() {
        return 0;
    }
}