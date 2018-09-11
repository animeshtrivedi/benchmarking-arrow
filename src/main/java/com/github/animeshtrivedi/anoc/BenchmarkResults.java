package com.github.animeshtrivedi.anoc;

/**
 * Created by atr on 11.09.18.
 */
abstract public class BenchmarkResults extends Thread{
    abstract long totalInts();
    abstract long totalLongs();
    abstract long totalFloat8();
    abstract long totalFloat4();
    abstract long totalBinary();
    abstract long totalBinarySize();
    abstract double getChecksum();
    long getTotalBytesRead(){
        long size = 0;
        size+= totalInts() * Integer.BYTES;
        size+= totalLongs() * Long.BYTES;
        size+= totalFloat4() * Float.BYTES;
        size+=totalFloat8() * Double.BYTES;
        size+=totalBinarySize();
        return size;
    }
}
