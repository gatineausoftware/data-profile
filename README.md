# data-profile




defn get-column [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   clojure.pprint/pprint)

### to run on spark cluster

spark-submit --class data_profile.core --master yarn target/data-profile-0.1.0-SNAPSHOT-standalone.jar \
s3n://ccnadl/raw/upc/yr=2015/mo=11/dy=19/upcs.csv 0

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



added a new function to file, does not work from repl.  works with lein run.
try closing connection, openning a new repl and re-connecting (or did i create connection first)....then it works.


tried adding new function to repl directly...also did not work


## next steps

1. can sparkling work with closures?
2. build function to track range of int and number of non parse.
3. write good data to s3...

def d2 [[1 3] [2 "blah"] [4 6] ["eak" 7]])

(def b2 [{:min 99 :max: -1 :bad 0} {:min 99 :max -1 :bad 0}])

(min 4 5)

(assoc-in {:min 4 :max 5} [:min] 4)

(def profile {:min -1 :max -1 :bad-count 0})

(:min {:min 5})

(if
(try (integer? (bigint "4"))
  (catch Exception e false)) "hi" "no")

(defn dp [a b]
  (if
    (try (integer? (bigint b))
      (catch Exception e false))
        (let [x (bigint b)]
        (->
         (assoc-in a [:min] (min (:min a) x))
         (assoc-in [:max] (max (:max a) x))
         ))
         (update-in a [:bad] inc)))




(reduce dp {:min 999999 :max -1} d)

(reduce (fn [a b]
          (map dp a b)) b2 d2)

