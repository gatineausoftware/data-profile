#Data Profiling
A _rough_ collection of utilities for checking data for schema errors. Based on Spark and Clojure.  You will need to install [leiningen](http://leiningen.org/).



## to run locally

lein run [command] [directory] [additional arguments]

when running locally, directory should be local.



## to run on cluster

#### Build uberjar

lein uberjar

#### submit to spark cluster
spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar [command] [directory] [additional arguments]

1. in this case cluster is AWS EMR cluster. 
2. directory can be s3 or hdfs.

### Examples
##### local
1. lein run check-schema netbase/ redshift.schema
2. lein run write-bad-data netbase/ redshift.schema output
3. lein run get-schema-errors netbase/ redshift.schema

##### on cluster
1. spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar write-bad-data s3n://ccnadl/raw/ahold/yr=2015/mo=11/dy=13/Coca_Cola_201349.csv  ahold.schema output



### to do
1. read schema in from a file, not hard-code
2. 