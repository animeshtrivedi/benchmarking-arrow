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
import org.apache.arrow.memory.*;
import org.apache.log4j.Logger;

public class TracerAllocator extends RootAllocator {
    final static Logger logger = Logger.getLogger(TracerAllocator.class);

    public TracerAllocator(final long limit) {
        super(limit);
    }

    public TracerAllocator(final AllocationListener listener, final long limit) {
        super(listener, limit);
    }

    @Override
    public ArrowBuf buffer(int i) {
        long s = System.nanoTime();
        ArrowBuf ret = super.buffer(i);
        long e = System.nanoTime();
        logger.debug(" buffer(int " + i + " ) took " + Utils.commaLongNumber(e-s) + " nanosec | plot " + (e-s));
        return ret;
    }

    @Override
    public ArrowBuf buffer(int i, BufferManager bufferManager) {
        long s = System.nanoTime();
        ArrowBuf ret = super.buffer(i, bufferManager);
        long e = System.nanoTime();
        logger.debug(" buffer(int " + i + " , BufferManager " + bufferManager +" ) took " + Utils.commaLongNumber(e-s) + " nanosec | plot " + (e-s));
        return ret;
    }

    @Override
    public ArrowByteBufAllocator getAsByteBufAllocator() {
        //called
        return super.getAsByteBufAllocator();
    }

    @Override
    public BufferAllocator newChildAllocator(String s, long l, long l1) {
        logger.debug(" xxx newChildAllocator(String s, long l, long l1) s " + s + " l " + l + " l1 " + l1);
        return super.newChildAllocator(s, l, l1);
    }

    @Override
    public BufferAllocator newChildAllocator(String s, AllocationListener allocationListener, long l, long l1) {
        logger.debug(" xxx newChildAllocator(String s, AllocationListener allocationListener, long l, long l1) s " + s + " l " + l + " l1 " + l1 + " allocationlisttner " + allocationListener);
        return super.newChildAllocator(s, allocationListener, l, l1);
    }

    @Override
    public void close() {
        logger.debug(" xxx close");
    }

    @Override
    public long getAllocatedMemory() {
        long ret = super.getAllocatedMemory();
        logger.debug(" xxx getAllocatedMemory " + ret);
        return ret;
    }

    @Override
    public long getLimit() {
        long ret = super.getLimit();
        logger.debug(" xxx getLimit " + ret);
        return ret;
    }

    @Override
    public void setLimit(long l) {
        logger.debug(" xxx setLimit " + l);
        super.setLimit(l);
    }

    @Override
    public long getPeakMemoryAllocation() {
        long ret = super.getPeakMemoryAllocation();
        logger.debug(" xxx getPeakMemoryAllocation " + ret);
        return ret;
    }

    @Override
    public long getHeadroom() {
        long ret = super.getHeadroom();
        logger.debug(" xxx getHeadroom " + ret);
        return ret;
    }

    @Override
    public AllocationReservation newReservation() {
        logger.debug(" xxx newReservation");
        return super.newReservation();
    }

    @Override
    public ArrowBuf getEmpty() {
        //called
        ArrowBuf ret = super.getEmpty();
        //logger.debug(" xxx getEmpty " + ret);
        return ret;
    }

    @Override
    public String getName() {
        //called
        String ret = super.getName();
        //logger.debug(" xxx getName " + ret);
        return ret;
    }

    @Override
    public boolean isOverLimit() {
        boolean ret = super.isOverLimit();
        logger.debug(" xxx isOverLimit " + ret);
        return ret;
    }

    @Override
    public String toVerboseString() {
        String ret = super.toVerboseString();
        logger.debug(" xxx toVerboseString " + ret);
        return ret;
    }

    @Override
    public void assertOpen() {
        //called
        super.assertOpen();
    }
}
