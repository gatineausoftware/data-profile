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
1. spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar write-bad-data s3n://path/yourfile.csv  ahold.schema output
2. spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar profile yourfile.csv


### to do
2. do i need a -main?  or can I run arbitrary functions on cluster?
3. do functions need to be compiled to execute on cluster?
5. get midje working. ** not working....problem with AOT? **
6. get repl development working ** Yes.  however cannot submit functions defined in REPL**
8. compare performance of clojure vs scala.
9. build function that outputs 'good' data and 'bad' data in separate locations.
10. clean up main loop.
11. add more metadata to schema, e.g., how is file delimited (comma, tab, | etc…)
12. read data from other file formats (ORC, Parquet?  Spark supports this, but is it realistic for raw data to come in in these formats?)
13. finish implementing schema (e.g., date)  **DATE is COMPLETE**
14. how to encode max_int in serialized schema?
15. be sure to cache rdd for sequences of operations.
17. re-name schemas, adopt a database/table convention.
18. autogenerate schema after profiling.
