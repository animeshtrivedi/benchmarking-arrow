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

import com.github.animeshtrivedi.generator.GeneratorFactory;
import org.apache.commons.cli.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class ParseOptions {
    private Options options;
    final static Logger logger = Logger.getLogger(ParquetToArrow.class);

    public ParseOptions(){
        options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "test to perform - ParquetToArrow OR ArrowRead (case in-sensitive).");
        options.addOption("i", "input", true, "input directory containing files.");
        options.addOption("o", "output", true, "output directory location.");
        options.addOption("w", "writeBufferSize", true, "write buffer size, default: 1MB");
        options.addOption("p", "parallel", true, "number of parallel instances");

        options.addOption("r", "rows", true, "rows per parallel worker");
        options.addOption("s", "size", true, "size for binary payload");
        options.addOption("n", "name", true, "int, or binary");
        options.addOption("c", "nulCols", true, "number of columns");
        options.addOption("g", "groupSize", true, "row group size (stepping) in Arrow");
        options.addOption("d", "debug", false, "debug mode");
        options.addOption("x", "warmup run", false, "do a warm-up");
        options.addOption("v", "verbose", false, "show some additional printouts");

        options.addOption("a", "on vs offheap", false, "-a enables offheap direct buffers, otherwise default is on-heap byte[]");
        options.addOption("b", "run gc", false, "-b enables running GC whenever sensible");
    }

    public void show_help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }
    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if (cmd.hasOption("t")) {
                Configuration.testName = cmd.getOptionValue("t").trim().toLowerCase();
                if(!Configuration.fileReadTests.contains(Configuration.testName))
                    Configuration.isFileReadingInvolved = false;
            }
            if (cmd.hasOption("i")) {
                Configuration.inputDir = cmd.getOptionValue("i").trim();
            }
            if (cmd.hasOption("o")) {
                Configuration.outputDir = cmd.getOptionValue("o").trim();
            }
            if (cmd.hasOption("d")) {
                Configuration.debug = true;
            }
            if (cmd.hasOption("v")) {
                Configuration.verbose = true;
            }
            if (cmd.hasOption("x")) {
                Configuration.warmup = true;
            }
            if (cmd.hasOption("a")) {
                Configuration.offHeap = true;
            }
            if (cmd.hasOption("b")) {
                Configuration.runGC = true;
            }
            if (cmd.hasOption("w")) {
                long sz = Integer.parseInt(cmd.getOptionValue("w").trim());
                if((sz & (sz -1)) != 0){
                    throw new ParseException(" please set the buffer size to the power of two.");
                }
                Configuration.setWriteBufferSize((int) sz);
            }
            if (cmd.hasOption("p")) {
                Configuration.parallel = Integer.parseInt(cmd.getOptionValue("p").trim());
            }

            if (cmd.hasOption("r")) {
                Configuration.rowsPerThread = Long.parseLong(cmd.getOptionValue("r").trim());
            }
            if (cmd.hasOption("s")) {
                Configuration.binSize = Integer.parseInt(cmd.getOptionValue("s").trim());
            }
            if (cmd.hasOption("n")) {
                 String name = cmd.getOptionValue("n").trim();
                 if(name.compareToIgnoreCase("int") == 0)
                     Configuration.type = GeneratorFactory.INT_GENERATOR;
                 else if (name.compareToIgnoreCase("binary") == 0) {
                    Configuration.type = GeneratorFactory.BIN_GENERATOR;
                } else {
                     throw new ParseException("Illegal name for a type " + name);
                 }
            }
            if (cmd.hasOption("c")) {
                Configuration.numCols = Integer.parseInt(cmd.getOptionValue("c").trim());
            }
            if (cmd.hasOption("g")) {
                Configuration.stepping = Integer.parseInt(cmd.getOptionValue("g").trim());
            }

        } catch (ParseException e) {
            System.err.println("Failed to parse command line properties" + e);
            show_help();
            System.exit(-1);
        }
    }
}
