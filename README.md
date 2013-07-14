BigFrame
========

BigFrame is a benchmark generator for big data analytics, which means that it can instantiate different kinds of benchmarks.
Unlike the exsiting benchmarks, e.g TPCDS, HiveBench, etc. which are either micro-benchmarks or benchmarks in 
very specific domains, making them not fit into the big data enviroment taday, BigFrame can instead generate the 
bechmark tailored to a specific set of data and workload requirements.
 
For example, some enterprises may be grappling with increasing data volumes; with the variety and velocity of their 
data not being pressing concerns. Some other enterprises may be interested in benchmarking systems that can handle 
larger volumes and variety of data; but with the volume of unstructured data dominating that of the structured data 
by orders of magnitude. A third category of enterprises may be interested in understanding reference architectures 
for data analytics applications that need to deal with large rates of streaming data (i.e., large velocity of data).

### For more information about BigFrame, please refer to the [wiki](https://github.com/bigframeteam/BigFrame/wiki).

Building
--------

Currently, BigFrame uses ant builder. To build BigFrame, just run ant at the root directory.  

BigFrame requires:
* JDK 1.6 is needed, JDK 1.7 is recommended.
* Hadoop 1.0.4 (other versions are not tested)


Configuration
--------

Before running BigFrame, you need to edit the `conf/config.sh` to set the following variables:
* `HADOOP_HOME`: By default, it tries to get it from the environment variables.
* `TPCDS_LOCAL`: A temp directory to store the imtermediate data for tpcds generator. 

Bechmark Specification:
--------
To customize the benchmark you want to generate, you can change the propoerties inside the `conf/bigframe-core.xml`.


Run
--------
Use command `./bin/datagen` to generate the data you need.
