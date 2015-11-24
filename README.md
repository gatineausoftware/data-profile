## to run locally

lein run <command> <filename> <other args>




## to run on cluster

lein uberjar

spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar write-bad-data s3n://ccnadl/raw/ahold/yr=2015/mo=11/dy=13/ bad-data

