This repository includes AsterixDB's source code with modifications made for supporting Dynamic Hybrid Hash Join various design strategies for a VLDB 2022 paper (https://arxiv.org/abs/2112.02480).

The following hints have been provided to guide the AsterixDB during the runtime to use the mentioned strategy:

data-insertion
victim-selection
GrowSteal
build-size
min-build-partitions

The values for data-insertion hint can be:
APPEND <integer>,
BESTFIT,
FIRSTFIT_N <integer>,
FIRSTFIT,
RANDOM_N <integer>,
NEXTFIT
 
 Example:
 
 select * from ds1, ds2 where ds1.unique1 /*+ data-insertion NEXTFIT */=ds2.unique1;
 
 The values for victim-selection hint can be:
 HALF_EMPTY,
 LARGEST_SIZE_SELF,
 LARGEST_RECORD_SELF,
 SMALLEST_SIZE_SELF,
 SMALLEST_RECORD_SELF,
 MEDIAN_SIZE_SELF,
 MEDIAN_RECORD_SELF,
 RANDOM_SELF,
 MAX_SIZE_MIN_RECORD_SELF,
 LARGEST_SIZE,
 LARGEST_RECORD,
 SMALLEST_SIZE,
 SMALLEST_RECORD,
 MEDIAN_SIZE,
 MEDIAN_RECORD,
 RANDOM,
 MAX_SIZE_MIN_RECORD //Record Size Ratio
 
The following hint can be used for enabling Grow-Steal, otherwise No Grow-No Steal will be used by default:
 GrowSteal
 
 

## Build from source

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.3.9 or newer.
* JDK 11 or newer.
* Python 3.6+ with pip and venv

Instructions for building the master:

* Checkout AsterixDB master:

        $git clone https://github.com/apache/asterixdb.git

* Build AsterixDB master:

        $cd asterixdb
        $mvn clean package -DskipTests


## Run the build on your machine
Here are steps to get AsterixDB running on your local machine:

* Start a single-machine AsterixDB instance:

        $cd asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-asterixdb-*-SNAPSHOT
        $./opt/local/bin/start-sample-cluster.sh

* Good to go and run queries in your browser at:

        http://localhost:19006

* Read more [documentation](https://ci.apache.org/projects/asterixdb/index.html) to learn the data model, query language, and how to create a cluster instance.

## Documentation

To generate the documentation, run asterix-doc with the generate.rr profile in maven, e.g  `mvn -Pgenerate.rr ...`
Be sure to run `mvn package` beforehand or run `mvn site` in asterix-lang-sqlpp to generate some resources that
are used in the documentation that are generated directly from the grammar.

* [master](https://ci.apache.org/projects/asterixdb/index.html) |
  [0.9.6](http://asterixdb.apache.org/docs/0.9.6/index.html) |
  [0.9.5](http://asterixdb.apache.org/docs/0.9.5/index.html) |
  [0.9.4.1](http://asterixdb.apache.org/docs/0.9.4.1/index.html) |
  [0.9.4](http://asterixdb.apache.org/docs/0.9.4/index.html) |
  [0.9.3](http://asterixdb.apache.org/docs/0.9.3/index.html) |
  [0.9.2](http://asterixdb.apache.org/docs/0.9.2/index.html) |
  [0.9.1](http://asterixdb.apache.org/docs/0.9.1/index.html) |
  [0.9.0](http://asterixdb.apache.org/docs/0.9.0/index.html)

## Community support

- __Users__</br>
maling list: [users@asterixdb.apache.org](mailto:users@asterixdb.apache.org)</br>
Join the list by sending an email to [users-subscribe@asterixdb.apache.org](mailto:users-subscribe@asterixdb.apache.org)</br>
- __Developers and contributors__</br>
mailing list:[dev@asterixdb.apache.org](mailto:dev@asterixdb.apache.org)</br>
Join the list by sending an email to [dev-subscribe@asterixdb.apache.org](mailto:dev-subscribe@asterixdb.apache.org)

