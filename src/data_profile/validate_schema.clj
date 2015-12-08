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


 (defn column-satisfies-schema? [a b]
   (case (:type a)
     :integer (if-let [i (getInteger b)]
                (and (<= i (:max a) ) (>= i (:min a)))
                false)
     :numeric (if-let [d (getDecimal b)]
                true false)
     :varchar (<= (count b) (:size a))
     :date (isDate? b)
     :decimal (if-let [d (getDecimal b)]
                (and (<= d (:max a)) (>= d (:min a)) (<= (.scale d) (:max_scale a))) false)
     true))


 (defn row-satisfies-schema? [schema row]
   (and (= (count schema) (count row))
     (every? true? (map column-satisfies-schema? schema row))))



  (defn check-integer [x min max]
    (if-let [i (getInteger x)]
        (if (or (< i min) (> i max))
           {:error :int_range} {:error :none}) {:error :non_int}))


  (defn check-varchar [x size]
      (if (> (count x) size)
        {:error :varchar_range} {:error :none}))

  (defn check-numeric [x]
    (if (isInteger? x)
      {:error :none} {:error :non_numeric}))

  ;;add decimal and date
  (defn validate-field [a b]
    (let [f {:name (:name a) :value b}]

     (case (:type a)
     :integer (conj f (check-integer b (:min a) (:max a)))
     :numeric (conj f (check-numeric b))
     :varchar (conj f (check-varchar b (:size a)))
     (conj f {:error :none}))))


  (defn get-schema-errors [schema row]
    (doall
     (->>
     (map validate-field schema row)
     (filter #(not= :none (:error %))))))


  ;;prints out validation errors of num_records that have schema errors
  (defn list-schema-errors [rdd schema-name {:keys [delimiter num-records]}]

   (->>
    rdd
    (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
    (spark/filter (complement (partial row-satisfies-schema? (get-schema schema-name))))
    (spark/take num-records)
    (map (partial get-schema-errors (get-schema schema-name)))))



  ;;prints out records that don't match schema
  (defn list-bad-records [rdd schema-name {:keys [delimiter num-records]}]
    (println schema-name)
    (->>
     rdd
     (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
     (spark/filter (complement (partial row-satisfies-schema? (get-schema schema-name))))
     (spark/take num-records)))

