BigFrame
========

BigFrame is a benchmark generator for big data analytics. Unlike the exsiting benchmark, e.g TPCDS, HiveBench, etc. which are either
micro-benchmark or benchmark in a very specific domain, and they are not fit into the big data enviroment taday, BigFrame is a benchmarking framework that can instantiate different benchmarks.

Each benchmark instantiated by the framework would be tailored to a specific set of data and workload requirements. For example, some enterprises may be 
grappling with increasing data volumes; with the variety and velocity of their data not being pressing concerns. Some other enterprises may be interested in 
benchmarking systems that can handle larger volumes and variety of data; but with the volume of unstructured data dominating that of the structured data 
by orders of magnitude. A third category of enterprises may be interested in understanding reference architectures for data analytics applications that need 
to deal with large rates of streaming data (i.e., large velocity of data).
