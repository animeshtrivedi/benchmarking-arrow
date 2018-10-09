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

import java.nio.channels.WritableByteChannel;

public class GeneratorFactory {
    static public final int INT_GENERATOR = 1;
    static public final int LONG_GENERATOR = 2;
    static public final int BIN_GENERATOR = 3;

    static public ArrowDataGenerator generator(WritableByteChannel channel) throws Exception {
        switch(Configuration.type){
            case INT_GENERATOR: return new IntegerGenerator(channel);
            case LONG_GENERATOR: return new LongGenerator(channel);
            case BIN_GENERATOR: return new BinaryGenerator(channel);
        }
        throw new Exception(" wrong tpye " + Configuration.type);
    }
}
