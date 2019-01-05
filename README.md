## Benchmarking Arrow  
test

## How to build 
```commandline
mvn package
mvn dependency:copy-dependencies  
```
### Generate TPC-DS dataset 
You can generate TPC-DS dataset using Spark as shown here: https://github.com/zrlio/parquet-generator 
 An example command to generate the `store_sales` table would be 
 
```concept
./bin/spark-submit -v  
--num-executors 1  
--executor-cores 16 
--executor-memory 128G  
--driver-memory 32G 
--driver-cores 8 
--master yarn  
--class com.ibm.crail.spark.tools.ParquetGenerator ~/your_location/parquet-generator-1.0.jar 
-c tpcds 
-o /sql/F10/ <-- output location on HDFS 
-p 1 
-t 1 
-tdsd ~/your_location/tpcds-kit/tools/  <-- location of your tpcds-toolkit  
-tdd 1 
-tff store_sales 
-tsf 10 <-- scale factor  
```

### Convert Parquet to Arrow 
Once you have to the files, you need to convert them to Arrow files. Here is how you do it: 
```concept
java -Xmn16G -Xmx128G -cp ./benchmark-arrow-1.0.jar:HADOOP_CLASSPATH:$CRAIL_CLASSPATH com.github.animeshtrivedi.anoc.Main -i hdfs://namenode:9000/input/store_sales -o crail://namenode:9060/output/store_sales/ -d hdfs -t ParquetToArrow -p 1
```
You can use the fully qualified HDFS or Crail name to make it the destination of the output path. 
`-p` says how many files are are there. These files will be processed in parallel. I typically use 
 `-p`equal to the number of cores I have in the system. 

### Benchmark Reading Arrow 
#### From HDFS
```concept
java -Xmn16G -Xmx128G -cp ./benchmark-arrow-1.0.jar:HADOOP_CLASSPATH:$CRAIL_CLASSPATH com.github.animeshtrivedi.anoc.Main  -i hdfs://namenode:9000/input/store_sales/ -d hdfs -t ArrowRead -p 16
```
Sample output: 
```bash
Test prep finished, starting the execution now ...
	 [0] totalRows: 18000399 || ints: 172712190 , long 18000399 , float4 0 , double 206280767 , binary 0 binarySize 0 || runtimeInNS 9868839944 , totalBytesProcessed 2485098088 , bandwidth 2.01 Gbps.
	 [1] totalRows: 18000398 || ints: 172719749 , long 18000398 , float4 0 , double 206288975 , binary 0 binarySize 0 || runtimeInNS 9791996373 , totalBytesProcessed 2485193980 , bandwidth 2.03 Gbps.
	 [2] totalRows: 18000398 || ints: 172707727 , long 18000398 , float4 0 , double 206273251 , binary 0 binarySize 0 || runtimeInNS 10118936751 , totalBytesProcessed 2485020100 , bandwidth 1.96 Gbps.
	 [3] totalRows: 18000399 || ints: 172713531 , long 18000399 , float4 0 , double 206286470 , binary 0 binarySize 0 || runtimeInNS 9459976910 , totalBytesProcessed 2485149076 , bandwidth 2.10 Gbps.
	 [4] totalRows: 18000400 || ints: 172710120 , long 18000400 , float4 0 , double 206282783 , binary 0 binarySize 0 || runtimeInNS 10077998215 , totalBytesProcessed 2485105944 , bandwidth 1.97 Gbps.
	 [5] totalRows: 18000401 || ints: 172712417 , long 18000401 , float4 0 , double 206282694 , binary 0 binarySize 0 || runtimeInNS 9821663982 , totalBytesProcessed 2485114428 , bandwidth 2.02 Gbps.
	 [6] totalRows: 18000402 || ints: 172711185 , long 18000402 , float4 0 , double 206284047 , binary 0 binarySize 0 || runtimeInNS 9449918251 , totalBytesProcessed 2485120332 , bandwidth 2.10 Gbps.
	 [7] totalRows: 18000402 || ints: 172719204 , long 18000402 , float4 0 , double 206296533 , binary 0 binarySize 0 || runtimeInNS 10258272075 , totalBytesProcessed 2485252296 , bandwidth 1.94 Gbps.
	 [8] totalRows: 18000401 || ints: 172716294 , long 18000401 , float4 0 , double 206284808 , binary 0 binarySize 0 || runtimeInNS 9250639649 , totalBytesProcessed 2485146848 , bandwidth 2.15 Gbps.
	 [9] totalRows: 18000399 || ints: 172718852 , long 18000399 , float4 0 , double 206293142 , binary 0 binarySize 0 || runtimeInNS 9775924549 , totalBytesProcessed 2485223736 , bandwidth 2.03 Gbps.
	 [10] totalRows: 18000399 || ints: 172709221 , long 18000399 , float4 0 , double 206276714 , binary 0 binarySize 0 || runtimeInNS 10027787087 , totalBytesProcessed 2485053788 , bandwidth 1.98 Gbps.
	 [11] totalRows: 18000397 || ints: 172721787 , long 18000397 , float4 0 , double 206294791 , binary 0 binarySize 0 || runtimeInNS 9760765562 , totalBytesProcessed 2485248652 , bandwidth 2.04 Gbps.
	 [12] totalRows: 18000399 || ints: 172707907 , long 18000399 , float4 0 , double 206279885 , binary 0 binarySize 0 || runtimeInNS 9901731300 , totalBytesProcessed 2485073900 , bandwidth 2.01 Gbps.
	 [13] totalRows: 18000398 || ints: 172720720 , long 18000398 , float4 0 , double 206292652 , binary 0 binarySize 0 || runtimeInNS 9651560281 , totalBytesProcessed 2485227280 , bandwidth 2.06 Gbps.
	 [14] totalRows: 18000398 || ints: 172716691 , long 18000398 , float4 0 , double 206289062 , binary 0 binarySize 0 || runtimeInNS 9564661512 , totalBytesProcessed 2485182444 , bandwidth 2.08 Gbps.
	 [15] totalRows: 18000398 || ints: 172714135 , long 18000398 , float4 0 , double 206283568 , binary 0 binarySize 0 || runtimeInNS 9613280146 , totalBytesProcessed 2485128268 , bandwidth 2.07 Gbps.
-----------------------------------------------------------------------
Total bytes: 39762339160(37GiB) bandwidth 30.99 Gbps
-----------------------------------------------------------------------
```
#### From Crail
```concept
java -Xmn16G -Xmx128G -cp ./benchmark-arrow-1.0.jar:HADOOP_CLASSPATH:$CRAIL_CLASSPATH com.github.animeshtrivedi.anoc.Main  -i hdfs://namenode:9000/input/store_sales/ -d hdfs -t ArrowRead -p 16
```
Sample output: 
```bash
Test prep finished, starting the execution now ...
	 [0] totalRows: 18000401 || ints: 172712417 , long 18000401 , float4 0 , double 206282694 , binary 0 binarySize 0 || runtimeInNS 8566642071 , totalBytesProcessed 2485114428 , bandwidth 2.32 Gbps.
	 [1] totalRows: 18000399 || ints: 172707907 , long 18000399 , float4 0 , double 206279885 , binary 0 binarySize 0 || runtimeInNS 8323743381 , totalBytesProcessed 2485073900 , bandwidth 2.39 Gbps.
	 [2] totalRows: 18000398 || ints: 172716691 , long 18000398 , float4 0 , double 206289062 , binary 0 binarySize 0 || runtimeInNS 8684245313 , totalBytesProcessed 2485182444 , bandwidth 2.29 Gbps.
	 [3] totalRows: 18000399 || ints: 172709221 , long 18000399 , float4 0 , double 206276714 , binary 0 binarySize 0 || runtimeInNS 8289986507 , totalBytesProcessed 2485053788 , bandwidth 2.40 Gbps.
	 [4] totalRows: 18000399 || ints: 172718852 , long 18000399 , float4 0 , double 206293142 , binary 0 binarySize 0 || runtimeInNS 8195776421 , totalBytesProcessed 2485223736 , bandwidth 2.43 Gbps.
	 [5] totalRows: 18000399 || ints: 172713531 , long 18000399 , float4 0 , double 206286470 , binary 0 binarySize 0 || runtimeInNS 8517283845 , totalBytesProcessed 2485149076 , bandwidth 2.33 Gbps.
	 [6] totalRows: 18000398 || ints: 172714135 , long 18000398 , float4 0 , double 206283568 , binary 0 binarySize 0 || runtimeInNS 8074775737 , totalBytesProcessed 2485128268 , bandwidth 2.46 Gbps.
	 [7] totalRows: 18000398 || ints: 172719749 , long 18000398 , float4 0 , double 206288975 , binary 0 binarySize 0 || runtimeInNS 8131236288 , totalBytesProcessed 2485193980 , bandwidth 2.45 Gbps.
	 [8] totalRows: 18000402 || ints: 172711185 , long 18000402 , float4 0 , double 206284047 , binary 0 binarySize 0 || runtimeInNS 8453482709 , totalBytesProcessed 2485120332 , bandwidth 2.35 Gbps.
	 [9] totalRows: 18000400 || ints: 172710120 , long 18000400 , float4 0 , double 206282783 , binary 0 binarySize 0 || runtimeInNS 8118978381 , totalBytesProcessed 2485105944 , bandwidth 2.45 Gbps.
	 [10] totalRows: 18000401 || ints: 172716294 , long 18000401 , float4 0 , double 206284808 , binary 0 binarySize 0 || runtimeInNS 8392061023 , totalBytesProcessed 2485146848 , bandwidth 2.37 Gbps.
	 [11] totalRows: 18000397 || ints: 172721787 , long 18000397 , float4 0 , double 206294791 , binary 0 binarySize 0 || runtimeInNS 8380084860 , totalBytesProcessed 2485248652 , bandwidth 2.37 Gbps.
	 [12] totalRows: 18000398 || ints: 172720720 , long 18000398 , float4 0 , double 206292652 , binary 0 binarySize 0 || runtimeInNS 8355813299 , totalBytesProcessed 2485227280 , bandwidth 2.38 Gbps.
	 [13] totalRows: 18000399 || ints: 172712190 , long 18000399 , float4 0 , double 206280767 , binary 0 binarySize 0 || runtimeInNS 8236475820 , totalBytesProcessed 2485098088 , bandwidth 2.41 Gbps.
	 [14] totalRows: 18000398 || ints: 172707727 , long 18000398 , float4 0 , double 206273251 , binary 0 binarySize 0 || runtimeInNS 8194077664 , totalBytesProcessed 2485020100 , bandwidth 2.43 Gbps.
	 [15] totalRows: 18000402 || ints: 172719204 , long 18000402 , float4 0 , double 206296533 , binary 0 binarySize 0 || runtimeInNS 8491232893 , totalBytesProcessed 2485252296 , bandwidth 2.34 Gbps.
-----------------------------------------------------------------------
Total bytes: 39762339160(37GiB) bandwidth 36.58 Gbps
-----------------------------------------------------------------------
```
#### From In-Memory 
```concept
java -Xmn16G -Xmx128G -cp ./benchmark-arrow-1.0.jar:HADOOP_CLASSPATH:$CRAIL_CLASSPATH com.github.animeshtrivedi.anoc.Main  -i hdfs://namenode:9000/input/store_sales/ -d hdfs -t ArrowRead -p 16
```

Sample output: 
```bash
Test prep finished, starting the execution now ...
	 [0] totalRows: 18000399 || ints: 172712190 , long 18000399 , float4 0 , double 206280767 , binary 0 binarySize 0 || runtimeInNS 7299193683 , totalBytesProcessed 2485098088 , bandwidth 2.72 Gbps.
	 [1] totalRows: 18000398 || ints: 172719749 , long 18000398 , float4 0 , double 206288975 , binary 0 binarySize 0 || runtimeInNS 7409396555 , totalBytesProcessed 2485193980 , bandwidth 2.68 Gbps.
	 [2] totalRows: 18000398 || ints: 172707727 , long 18000398 , float4 0 , double 206273251 , binary 0 binarySize 0 || runtimeInNS 7627628111 , totalBytesProcessed 2485020100 , bandwidth 2.61 Gbps.
	 [3] totalRows: 18000399 || ints: 172713531 , long 18000399 , float4 0 , double 206286470 , binary 0 binarySize 0 || runtimeInNS 7102838237 , totalBytesProcessed 2485149076 , bandwidth 2.80 Gbps.
	 [4] totalRows: 18000400 || ints: 172710120 , long 18000400 , float4 0 , double 206282783 , binary 0 binarySize 0 || runtimeInNS 7029802449 , totalBytesProcessed 2485105944 , bandwidth 2.83 Gbps.
	 [5] totalRows: 18000401 || ints: 172712417 , long 18000401 , float4 0 , double 206282694 , binary 0 binarySize 0 || runtimeInNS 7230067477 , totalBytesProcessed 2485114428 , bandwidth 2.75 Gbps.
	 [6] totalRows: 18000402 || ints: 172711185 , long 18000402 , float4 0 , double 206284047 , binary 0 binarySize 0 || runtimeInNS 7421455633 , totalBytesProcessed 2485120332 , bandwidth 2.68 Gbps.
	 [7] totalRows: 18000402 || ints: 172719204 , long 18000402 , float4 0 , double 206296533 , binary 0 binarySize 0 || runtimeInNS 7129311386 , totalBytesProcessed 2485252296 , bandwidth 2.79 Gbps.
	 [8] totalRows: 18000401 || ints: 172716294 , long 18000401 , float4 0 , double 206284808 , binary 0 binarySize 0 || runtimeInNS 7505656801 , totalBytesProcessed 2485146848 , bandwidth 2.65 Gbps.
	 [9] totalRows: 18000399 || ints: 172718852 , long 18000399 , float4 0 , double 206293142 , binary 0 binarySize 0 || runtimeInNS 7287457687 , totalBytesProcessed 2485223736 , bandwidth 2.73 Gbps.
	 [10] totalRows: 18000399 || ints: 172709221 , long 18000399 , float4 0 , double 206276714 , binary 0 binarySize 0 || runtimeInNS 7501525287 , totalBytesProcessed 2485053788 , bandwidth 2.65 Gbps.
	 [11] totalRows: 18000397 || ints: 172721787 , long 18000397 , float4 0 , double 206294791 , binary 0 binarySize 0 || runtimeInNS 7684682262 , totalBytesProcessed 2485248652 , bandwidth 2.59 Gbps.
	 [12] totalRows: 18000399 || ints: 172707907 , long 18000399 , float4 0 , double 206279885 , binary 0 binarySize 0 || runtimeInNS 7172358912 , totalBytesProcessed 2485073900 , bandwidth 2.77 Gbps.
	 [13] totalRows: 18000398 || ints: 172720720 , long 18000398 , float4 0 , double 206292652 , binary 0 binarySize 0 || runtimeInNS 7935776805 , totalBytesProcessed 2485227280 , bandwidth 2.51 Gbps.
	 [14] totalRows: 18000398 || ints: 172716691 , long 18000398 , float4 0 , double 206289062 , binary 0 binarySize 0 || runtimeInNS 7050279418 , totalBytesProcessed 2485182444 , bandwidth 2.82 Gbps.
	 [15] totalRows: 18000398 || ints: 172714135 , long 18000398 , float4 0 , double 206283568 , binary 0 binarySize 0 || runtimeInNS 8088240356 , totalBytesProcessed 2485128268 , bandwidth 2.46 Gbps.
-----------------------------------------------------------------------
Total bytes: 39762339160(37GiB) bandwidth 39.29 Gbps
-----------------------------------------------------------------------
```
