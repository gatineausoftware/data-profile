# data-profile




defn get-column [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   clojure.pprint/pprint)

### to run on spark cluster

spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar \
s3n://ccnadl/raw/upc/yr=2015/mo=11/dy=19/upcs.csv

success!


### to run locally
lein run resources/upcs.csv

success1


## from repl
lein repl

works!


### from lighttable

1. establish connection
2. open insta-repl and connect to data-profile

use 'data-profile.core)


(def sc (make-spark-context))

(get-column sc "resources/upcs.csv")

success!! [note that it prints on console]


(get-sample ....) also seems to work, but doens't generate a sample but the whole file.



added a new function, does not work from repl.  works with lein run.
try closing connection, openning a new repl and re-connecting (or did i create connection first)....then it works.



