(ns data-profile.core
(:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.string :as str])

  (:gen-class))


(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "data-profile"))]
    (spark/spark-context c)))




(defn count-records [sc filename]
	(->>
	(spark/text-file sc filename)
	(spark/map (fn [l] 1))
	(spark/reduce +)
	clojure.pprint/pprint))



(defn print-file [sc filename]
	(->>
    (spark/text-file sc filename)
	  spark/collect
	  clojure.pprint/pprint))



 ;;this doesn't work.  returns the whole file
(defn get-sample [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/sample true 10 7)
   (spark/collect)))


;;plow through exceptions
(defn  string->integer [s]
    (try
      (bigint s)
      (catch Exception e 0)))



(defn get-row [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   (clojure.pprint/pprint)))


;;skip incomplete records...or should i just filter first?
(defn getcolumn [n row]
    (->
       (str/split row #",")
       (try
         (nth n)
         (catch Exception e nil))
     ))


(defn get-average [sc filename n]
  (let [d (spark/text-file sc filename)
        c (spark/count d)
        s (->>
            (spark/map (partial getcolumn n))
            (spark/map bigint)
            (spark/reduce +))
        ]
    (clojure.pprint/pprint (/ s c))))


(defn get-incomplete-records [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
     spark/collect
     clojure.pprint/pprint))


(defn count-incomplete-records [sc filename n]
   (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
    (spark/count)
    (clojure.pprint/pprint)
    ))


;;only look at valid records
  (defn max-col-val [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map (partial getcolumn n))
    (spark/map string->integer)
    (spark/reduce max)
    (clojure.pprint/pprint)))


 (defn count-num-records [sc filename]
   (->> (spark/text-file sc filename)
         spark/count
        (clojure.pprint/pprint)))


 (defn get-max-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce max)
   (clojure.pprint/pprint)))


 ;;could change this to count distribution of number of columns
 (defn get-min-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce min)
   (clojure.pprint/pprint)))





(defn -main
  [command filename & args]
  (let [sc (make-spark-context)]
  (case command
    "count" (count-num-records sc filename)
    "max-col-val" (max-col-val sc filename (bigint (first args)))
    "max-col-count" (get-max-columns sc filename)
    "min-col-count" (get-min-columns sc filename)
    "count-incomplete-rows" (count-incomplete-records sc filename (bigint (first args)))
    "get-incomplete-records" (get-incomplete-records sc filename (bigint (first args)))
    (println "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]"))))







