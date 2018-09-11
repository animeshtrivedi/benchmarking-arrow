package com.github.animeshtrivedi.anoc;

/**
 * Created by atr on 11.09.18.
 */
public class BenchmarkConfiguration {
    // 1 MB writing buffer size
    static int writeBufferSize = 1024 * 1024;
    // write to a Crail, HDFS or local file system?
    static String[] validDestinations ={"hdfs", "crail", "local"};
    static String destination = "hdfs";
    // max fixed-size byte array width
    static int maxByteWidth = 8;
    // which test to do
    static String[] validTests ={"parquetToArrow", "arrowRead"};
    static String testName = "parquetToArrow";
}
