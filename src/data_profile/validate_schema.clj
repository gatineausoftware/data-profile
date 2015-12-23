(ns data-profile.validate-schema
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [clojure.java.io :as io]
              [data-profile.convert-hive-schema :as hive])
  (:use       [data-profile.util]
              [data-profile.profile]))



 (defn get-schema [name]
   (read-string (slurp (io/resource name))))


 (defn valid-column? [a b]
    (if (empty? b)
    true
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
     true)))


 (defn valid-row? [schema row]
   (and (= (count schema) (count row))
     (every? true? (map valid-column? schema row))))


  (defn check-integer [x min max]
    (if-let [i (getInteger x)]
      (->
       []
       (#(if (< i min) (conj % :min_range_error) %))
       (#(if (> i max) (conj % :max_range_error) %))
       )
      [:int_format]))


  (defn check-decimal [x min max scale]
    (if-let [d (getDecimal x)]
      (->
       []
       (#(if (< d min) (conj % :min_range_error) %))
       (#(if (> d max) (conj % :max_range_error) %))
       (#(if (> (.scale d) scale) (conj % :scale_error) %))
       )
      [:decimal_format]))


  (defn check-date [x]
    (if (isDate? x) [] [:date_format]))

  (defn check-numeric [x]
    (if (isInteger? x) [] [:number_format]))


  (defn check-varchar [x size]
      (if (> (count x) size)
        [:varchar_range] []))



  (defn validate-field [a b]
    (let [f {:column (:name a) :value b}]
     (conj f {:error
        (if (empty? b) []
         (case (:type a)
           :integer (check-integer b (:min a) (:max a))
           :numeric (check-numeric b)
           :varchar (check-varchar b (:size a))
           :decimal (check-decimal b (:min a) (:max a) (:max_scale a))
           :date (check-date b)
           []))})))




  (defn get-schema-errors [schema row]
    (doall
     (->>
     (map validate-field schema row)
     (filter #((complement empty?) (:error %))))))


  ;;prints out validation errors of num_records that have schema errors
  (defn list-schema-errors [rdd schema-name {:keys [delimiter num-records]}]
   (let [schema (get-schema schema-name)]
   (->>
    rdd
    (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
    (spark/filter (complement (partial valid-row? schema)))
    (spark/take num-records)
    (map (partial get-schema-errors schema)))))



  (defn list-schema-errors-hcat [rdd database table {:keys [delimiter num-records]}]
    (let [c (read-string (slurp (io/resource "hcat_config.edn")))
          schema (hive/get-schema (c :server) (c :port) database table (c :user))]
      (->>
        rdd
        (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
        (spark/filter (complement (partial valid-row? schema)))
        (spark/take num-records)
        (map (partial get-schema-errors schema)))))


  ;;prints out records that don't match schema
  (defn list-bad-records [rdd schema-name {:keys [delimiter num-records]}]
    (->>
     rdd
     (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
     (spark/filter (complement (partial valid-row? (get-schema schema-name))))
     (spark/take num-records)))



  (defn list-bad-records-hcat [rdd database table {:keys [delimiter num-records]}]
    (let [c (read-string (slurp (io/resource "hcat_config.edn")))]
    (->>
     rdd
     (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
     (spark/filter (complement (partial valid-row? (hive/get-schema (c :server) (c :port) database table (c: :user)))))
     (spark/take num-records))))






