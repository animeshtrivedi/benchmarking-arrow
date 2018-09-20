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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;

import java.nio.channels.WritableByteChannel;

public class IntegerGenerator extends ArrowDataGenerator {
    int totalInts;

    public IntegerGenerator(WritableByteChannel channel) {
        super(channel);
        try {
            super.makeArrowSchema("", Types.MinorType.INT);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.totalInts = 0;
    }

    @Override
    void fillup(int count, FieldVector vector){
        IntVector intVector = (IntVector) vector;
        for (int i = 0; i < count; i++) {
         intVector.setSafe(i, 1, 42);
        }
        this.totalInts+=count;
    }

    @Override
    public long totalInts() {
        return this.totalInts;
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
