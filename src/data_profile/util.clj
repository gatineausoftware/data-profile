(ns data-profile.util
(:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.string :as str]
            [clj-time.core :as t]
            [clj-time.format :as f]))


(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "data-profile"))]
    (spark/spark-context c)))



;tried to include "MM/dd/yy" but it didn't work
(def date-parser (f/formatter (t/default-time-zone) "YYYY-MM-dd" "YYYY/MM/dd" "MM/dd/yyyy"))


(defn isDate? [x]
  (some? (try (f/parse date-parser x)
    (catch Exception e nil))))



(defn is-integer? [x]
   (try (bigint x)
    (catch Exception e false)))


;;return 0 if field is not an integer
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



;;skip incomplete records...or should i just filter first?
(defn getcolumn [n row]
  (let [s (str/split row #",")]
    (try
      (nth s n)
      (catch Exception e nil))))




(defn get-row [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   ))



(defn get-average [rdd column]
  (let [c (spark/count rdd)
        s (->>
            rdd
            (spark/map (partial getcolumn column))
            (spark/map bigint)
            (spark/reduce +))
        ]
    (/ s c)))



(defn get-incomplete-records [rdd n]
  (->>
    rdd
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
     spark/collect
    ))


(defn count-incomplete-records [rdd n]
   (->>
    rdd
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
    (spark/count)
    ))


;;only look at valid records
  (defn max-col-val [rdd n]
  (->>
    rdd
    (spark/map (partial getcolumn n))
    (spark/map string->integer)
    (spark/reduce max)))


 (defn count-num-records [rdd]
   (->> rdd
         spark/count
    ))


 (defn get-max-columns [rdd]
   (->>
    rdd
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce max)))



 ;;could change this to count distribution of number of columns
 (defn get-min-columns [rdd]
   (->>
    rdd
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce min)
   ))


 ;;bit of nonesense to handle key-value pairs
 (defn get-num-col-dist [rdd]
    (->>
     rdd
    (spark/map #(str/split % #","))
    (spark/map-to-pair (fn [r] (spark/tuple (count r) 1)))
    (spark/reduce-by-key +)
    (spark/map (s-de/key-value-fn (fn [k v] [k v])))
     spark/collect))
