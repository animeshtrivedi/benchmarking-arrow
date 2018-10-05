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

import com.github.animeshtrivedi.benchmark.Configuration;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.Types;

import java.nio.channels.WritableByteChannel;

public class BinaryGenerator extends ArrowDataGenerator {
    private int binSize;
    private byte[] bin;
    private long totalRows;

    public BinaryGenerator(WritableByteChannel channel) {
        super(channel);
        try {
            super.makeArrowSchema("", Types.MinorType.VARBINARY);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.binSize = Configuration.binSize;
        this.bin = new byte[this.binSize];
        this.totalRows = 0;
    }

    @Override
    int fillBatch(int startIndex, int endIndex, FieldVector vector){
        VarBinaryVector binVector = (VarBinaryVector) vector;
        for (int i = startIndex; i < endIndex; i++) {
            binVector.setSafe(i, bin);
        }
        totalRows+=(endIndex - startIndex);
        return (endIndex - startIndex);
    }

    @Override
    public long totalInts() {
        return 0;
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
        return this.totalRows;
    }

    @Override
    public long totalBinarySize() {
        return (this.totalRows * this.binSize);
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
