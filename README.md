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
1. lein run list-bad-records resources/sample2.csv testschema2.schema
2. lein run profile resources/sample.csv


##### on cluster
1. spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar write-bad-data s3n://path/yourfile.csv  ahold.schema output
2. spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar profile s3n://ccnadl/raw/crest/ccna_rstr_basc/t_fact_food_summary/yr=2015/mo=10/dy=01/xaf -s 0.01 -d \|


### profile notes:
- calculate % for different types?
- infer delimter?
- create a schema based on profile.



### to do
2. rather unhappy error message regarding kryo serialization if sample size is too big for profiling
3. do functions need to be compiled to execute on cluster?
6. get repl development working ** Yes.  however cannot submit functions defined in REPL**
8. compare performance of clojure vs scala.
9. build function that outputs 'good' data and 'bad' data in separate locations. ** DONE, except need to dig into write-csv, adds extra blank lines **
10. use 'components' to manage spark context?
11. add more metadata to schema, e.g., how is file delimited (comma, tab, | etc…)
12. read data from other file formats (ORC, Parquet?  Spark supports this, but is it realistic for raw data to come in in these formats?)
15. be sure to cache rdd for sequences of operations.
17. re-name schemas, adopt a database/table convention.
18. autogenerate schema after profiling.
19. process directories recursively
