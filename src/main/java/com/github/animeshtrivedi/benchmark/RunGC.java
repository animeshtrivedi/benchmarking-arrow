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

public class RunGC {
    private Thread tx;
    private RunGC(){}
    static private RunGC ourInstance = new RunGC();

    private class _RunGC implements Runnable {

        @Override
        public void run() {
            System.gc();
        }
    }

    static public RunGC getInstance() {
        return ourInstance;
    }

    synchronized void runGCAsync(){
        if(Configuration.runGC && this.tx == null){
            this.tx = new Thread(new _RunGC());
            this.tx.start();
        }
    }

    synchronized void waitForGC(){
        if(Configuration.runGC && this.tx != null){
            try {
                this.tx.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            this.tx = null;
        }
    }

    void runGC(){
        if(Configuration.runGC){
            System.gc();
        }
    }
}
