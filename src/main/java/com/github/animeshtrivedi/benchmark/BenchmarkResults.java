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

abstract public class BenchmarkResults implements DataInterface {
    protected BenchmarkResults(){
        this.longCount = 0;
        this.intCount = 0;
        this.float4Count = 0;
        this.float8Count = 0;
        this.binaryCount = 0;
        this.binarySizeCount = 0;
        this.totalRows = 0;
        this.checksum = 0;
        this.runtimeInNS = 0;
    }
    protected long longCount;
    protected long intCount;
    protected long float4Count;
    protected long float8Count;
    protected long binaryCount;
    protected long binarySizeCount;
    protected double checksum;
    protected long totalRows;
    protected long runtimeInNS;

    public long totalInts(){
        return intCount;
    }

    public long totalLongs(){
        return longCount;
    }

    public long totalFloat8(){
        return float8Count;
    }

    public long totalFloat4() {
        return float4Count;
    }
    public long totalBinary(){
        return binaryCount;
    }

    public long totalBinarySize() {
        return binarySizeCount;
    }

    public long totalRows(){
        return totalRows;
    }

    public double getChecksum(){
        return this.checksum;
    }

    public long getRunTimeinNS() {
        return this.runtimeInNS;
    }
}
