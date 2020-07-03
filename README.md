# PMwCAS benchmark
[![Build Status](https://dev.azure.com/haoxiangpeng/pmwcas/_apis/build/status/sfu-dis.pmwcas-benchmarks?branchName=master)](https://dev.azure.com/haoxiangpeng/pmwcas/_build/latest?definitionId=5&branchName=master)

PMwCAS is a library that allows atomically changing multiple 8-byte words on non-volatile memory in a lock-free manner. It allows developers to easily build lock-free data structures for non-volatile memory and requires no custom recovery logic from the application. More details are described in the following [slide deck](http://www.cs.sfu.ca/~tzwang/pmwcas-slides.pdf), [full paper](http://justinlevandoski.org/papers/ICDE18_mwcas.pdf) and [extended abstract](http://www.cs.sfu.ca/~tzwang/pmwcas-nvmw.pdf):

```
Easy Lock-Free Indexing in Non-Volatile Memory.
Tianzheng Wang, Justin Levandoski and Paul Larson.
ICDE 2018.
```
```
Easy Lock-Free Programming in Non-Volatile Memory.
Tianzheng Wang, Justin Levandoski and Paul Larson.
NVMW 2019.
Finalist for Memorable Paper Award.
```

## Benchmark

This repo includes all the benchmark code related to [PMwCAS](https://github.com/sfu-dis/pmwcas), 
specifically, we have `array_bench`, `doubly linked list`, and `skip-list`.
The `array_bench` performance is continuously tracked online at https://sfu-dis.github.io/pmwcas-benchmarks

We benchmark PMwCAS use three data structures: array, doubly-linked list and the skip list.
We choose these data structures because they provides different level of insights,
from concrete low level implementation to high level programming abstraction.

The array benchmark illustrated the basic simple usage of PMwCAS: 
users want to atomically update multiple memory locations.
This mini benchmark shows the basic performance impact of PMwCAS.
Through representative workloads and careful performance analysis,
we show the runtime cost of PMwCAS, cost of persistency and the cost of persistent memory versus the DRAM.
Users with occasional needs for multi-word updates may find the benchmark useful.

We also see the huge demand for the industry and academia to migrate existing DRAM data structures to persistent memory.
The second use case, doubly linked/skip list demonstrates exactly this scenario,
where we try to implement a doubly-linked/skip list with PMwCAS.
We argue that PMwCAS allows non-expert programmers to migrate DRAM data structures to persistent memory data structures with relatively small mental burden.
The PMwCAS API allows them to implement safe and efficient data structures on persistent memory.


## Build
```bash
mkdir build && cd build
cmake -DPMEM_BACKEND=[PMDK/Volatile/Emu] \ # persistent memory backend
      -DDESC_CAP=[4/5/6/...] \ # descriptor capacity
      -DWITH_RTM=[0/1] \ # Use Intel RTX
      ..
```
For more detailed dependencies and build instruction, checkout the [Dockerfile](https://github.com/sfu-dis/pmwcas-benchmarks/blob/master/Dockerfile). 


