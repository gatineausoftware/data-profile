(ns data-profile.core
(:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.string :as str]
            [clojure-csv.core :as csv]
            [data-profile.schema :as schema])
  (:use [data-profile.util])

  (:gen-class))

;; verifies that a columm satisfies schema, returns true or false
 (defn columm-satisfies-schema? [a b]
   (case (:type a)
     :integer (if (is-integer? b)
                (and (<= (bigint b) (:max a) ) (>= (bigint b) (:min a)))
                false)
     :numeric (if (is-integer? b)
                true)
     :varchar (if (<= (count b) (:size a)) true false)


     true))


 (defn row-satisfies-schema? [schema row]
   (if (= (count schema) (count row))
     (every? true? (map columm-satisfies-schema? schema row))
     false))


 (defn get-bad-rows [rdd schema]
    (->>
     rdd
    (spark/map #(first (csv/parse-csv %)))
    (spark/filter  (complement (partial row-satisfies-schema? schema)))))


  ;; may need to use csv/write-csv
 (defn write-bad-data [rdd schema output]
   (->>
    (get-bad-rows rdd schema)
    (spark/map #(apply str (interpose "," %)))
    (spark/save-as-text-file output)))


  (defn check-integer [x min max]
    (if (is-integer? x)
          (if (or (x < min) (x > max))
            {:error :int_range} {:error :none}) {:error :non_int}))


  (defn check-varchar [x size]
      (if (> (count x) size)
        {:error :varchar_range} {:error :none}))

  (defn check-numeric [x]
    (if (is-integer? x)
      {:error :none} {:error :non_numeric}))


  (defn validate-field [a b]
    (let [f {:name (:name a) :value b}]

     (case (:type a)
     :integer (conj f (check-integer b (:min a) (:max a)))
     :numeric (conj f (check-numeric b))
     :varchar (conj f (check-varchar b (:size a)))
     (conj f {:error :none}))))


  (defn validate-row [schema row]
    (->>
     (map validate-field schema row)
     (filter #(not= :none (:error %)))))

 (defn get-schema-errors [rdd schema]
   (->>
    (get-bad-rows rdd schema)
    (spark/map (partial validate-row schema))
    (spark/take 100)

    ))


 (defn check-schema [rdd schema]
   (->>
     rdd
    (spark/map #(first (csv/parse-csv %)))
    (spark/take 1)
    first
    (map validate-field schema)))



(defn -main
  [command filename & args]
  (let [sc (make-spark-context)
        rdd (spark/text-file sc filename)]
  (->>
  (case command
    "count" (count-num-records rdd)
    "max-col-val" (max-col-val rdd (bigint (first args)))
    "max-col-count" (get-max-columns rdd)
    "min-col-count" (get-min-columns rdd)
    "count-incomplete-rows" (count-incomplete-records rdd (bigint (first args)))
    "get-incomplete-records" (get-incomplete-records rdd (bigint (first args)))
    "get-num-col-dist" (get-num-col-dist rdd)
    "write-bad-data" (write-bad-data rdd (schema/get-schema "redshift") (first args))
    "get-schema-errors" (get-schema-errors rdd (schema/get-schema "redshift"))
    "check-schema" (check-schema rdd (schema/get-schema "redshift"))
    "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]")
   clojure.pprint/pprint)))








