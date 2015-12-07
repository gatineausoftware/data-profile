(ns data-profile.validate-schema
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [clojure.java.io :as io])
  (:use       [data-profile.util]
              [data-profile.profile]))

(defn get-schema [name]
  (read-string (slurp (io/resource name))))


;; verifies that a columm satisfies schema, returns true or false
;should use if-let here
;;need to add decimal

 (defn column-satisfies-schema? [a b]
   (case (:type a)
     :integer (if (isInteger? b)
                (and (<= (bigint b) (:max a) ) (>= (bigint b) (:min a)))
                false)
     :numeric (if (isInteger? b)
                true)
     :varchar (if (<= (count b) (:size a)) true false)
     :date (isDate? b)
     :decimal (if-let [d (getDecimal b)]
                (and (<= d (:max a)) (>= d (:min a)) (<= (.scale d) (:max_scale a))) false)
     true))


 (defn row-satisfies-schema? [schema row]
   (if (= (count schema) (count row))
     (every? true? (map column-satisfies-schema? schema row))
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
    (if (isInteger? x)
          (if (or (< (getInteger x) min) (> (getInteger x) max))
            {:error :int_range} {:error :none}) {:error :non_int}))


  (defn check-varchar [x size]
      (if (> (count x) size)
        {:error :varchar_range} {:error :none}))

  (defn check-numeric [x]
    (if (isInteger? x)
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
