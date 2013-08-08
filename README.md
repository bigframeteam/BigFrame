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

To build BigFrame, run following commands from BigFrame home directory

1. `sbt/sbt compile`
2. `sbt/sbt package`
3. `sbt/sbt assembly`

BigFrame requires:
* JDK 1.6 is needed, JDK 1.7 is recommended.
* Hadoop 1.0.4 (other versions are not tested)
* Spark 0.7.3
* Scala 2.9.3 (required by Spark)


Configuration
--------

`/launcher/conf` directory contains following configuration files:

1. `bigframe-core.xml` : Specification of benchmark goes here. 
2. `config.sh` : A set of parameters required for data generation.
3. `spark-env.sh` : A set of parameters required for Spark execution.

Data Generation
--------

Use command `./launcher/bin/datagen` to generate the data you need.


Spark Workflow
--------

Run `./launcher/bin/run_spark` without any arguments. The command usage will be printed out.

Three workflows are supported at present:

1. "Relational only": It runs a query to find out total sales of products within each promotion.
2. "Text only": It runs a script to find out sentiment values for certain products by summing over all available tweets.
3. "Mixed": It runs a workflow involving relational, text and graph processing to generate a report specifying total sales and average sentiment for products that are promoted.
