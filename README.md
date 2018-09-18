## Benchmarking Arrow on Crail 

1 non-branch code (what is the cost of branching)

2 what is the cost of int, float, long materialization - as arrow as uses byte[] and then Platform class

3 1MB to 1GB buffer size - 64MB performance drops to 30 from 38-29 on 1MB buffer

4 Crail sanity performance 
./bin/crail iobench -e 1 -f /30gb -k $((30*1024)) -m true -o true -s $((1024*1024)) -t write
./bin/crail iobench -e 2 -f /30gb -k $((30*1024)) -m true -o true -s $((1024*1024)) -t readRandom #Sequential
Sequential access gives 97 Gbps 
Random access on 1MB aligned buffer gives ~45 Gbps, and for small request can degrade to 15Gbps 

Lets establish the in-memory performance 