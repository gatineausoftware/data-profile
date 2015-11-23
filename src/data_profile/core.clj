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



;;plow through exceptions
(defn  string->integer [s]
    (try
      (bigint s)
      (catch Exception e 0)))




 ;;this doesn't work.  returns the whole file
(defn get-sample [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/sample true 10 7)
   (spark/collect)))




(defn get-row [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   ))


;;skip incomplete records...or should i just filter first?
(defn getcolumn [n row]
  (let [s (str/split row #",")]
    (try
      (nth s n)
      (catch Exception e nil))))




(defn get-average [sc filename n]
  (let [d (spark/text-file sc filename)
        c (spark/count d)
        s (->>
            (spark/map (partial getcolumn n))
            (spark/map bigint)
            (spark/reduce +))
        ]
    (/ s c)))


(defn get-incomplete-records [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
     spark/collect
    ))


(defn count-incomplete-records [sc filename n]
   (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
    (spark/count)
    ))


;;only look at valid records
  (defn max-col-val [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map (partial getcolumn n))
    (spark/map string->integer)
    (spark/reduce max)))


 (defn count-num-records [sc filename]
   (->> (spark/text-file sc filename)
         spark/count
    ))


 (defn get-max-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce max)))



 ;;could change this to count distribution of number of columns
 (defn get-min-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce min)
   ))


 ;;bit of nonesense to handle key-value pairs
 (defn get-num-col-dist [sc filename]
    (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/map-to-pair (fn [r] (spark/tuple (count r) 1)))
    (spark/reduce-by-key +)
    (spark/map (s-de/key-value-fn (fn [k v] [k v])))
     spark/collect
     ))

(defn -main
  [command filename & args]
  (let [sc (make-spark-context)]
  (->>
  (case command
    "count" (count-num-records sc filename)
    "max-col-val" (max-col-val sc filename (bigint (first args)))
    "max-col-count" (get-max-columns sc filename)
    "min-col-count" (get-min-columns sc filename)
    "count-incomplete-rows" (count-incomplete-records sc filename (bigint (first args)))
    "get-incomplete-records" (get-incomplete-records sc filename (bigint (first args)))
    "get-num-col-dist" (get-num-col-dist sc filename)
    "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]")
   clojure.pprint/pprint)))








