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


(defn get-sample [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/sample true 10 7)
   (spark/collect)))




(defn get-column [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   (clojure.pprint/pprint)))



(defn get-average [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map first)
   (spark/map bigdec)
   (spark/reduce +)
   (clojure.pprint/pprint)))


  (defn getcolumn [n row]
    (->
       (str/split row #",")
       (nth n)
     ))


  (defn get-max [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map (partial getcolumn n))
    (spark/map bigint)
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


 (defn get-min-columns [sc filename]
   (->>
   (spark/text-file sc filename)
    (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce min)
    (clojure.pprint/pprint)))

 (defn count-incomplete-columns [sc filename n]
   (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(= n (count %)))
    (spark/count)
    (clojure.pprint/pprint)
    ))



(defn -main
  [command filename & args]
  (let [sc (make-spark-context)]
  (case command
    "count" (count-num-records sc filename)
    "max" (get-max sc filename (bigint (first args)))
    "max-col" (get-max-columns sc filename)
    "min-col" (get-min-columns sc filename)
    "count-incomplete" (count-incomplete-columns sc filename (first args))
    "valid commmans are: count, max")))







