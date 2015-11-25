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
lein run check-schema netbase/ resources/redshift.schema




### to do
1. read schema in from a file, not hard-code
2. 