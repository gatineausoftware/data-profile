(ns data-profile.localprofile
  (:require   [clojure.string :as string]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema])
  (:use       [data-profile.util]))


;;this version of profile just uses spark to pull back a sample of data.  profiling is then executed in driver




 (def a {:missing 0 :date {:count 0 :min -1 :max 1} :integer {:count 0 :min 0 :max 0} :string {:max_length 0}})

 (defn  get-integer [s]
    (try
      (bigint s)
      (catch Exception e nil)))


 (defn profile-missing [column c-profile]
   (if (zero? (count column))
              (update-in c-profile [:missing] + 1) c-profile))


 (defn profile-integer [column c-profile]
  (if-let [int-col (get-integer column)]
   (->
      c-profile
     (update-in [:integer :count] + 1)
     (update-in [:integer :max] max int-col)
     (update-in [:integer :min] min int-col)
    )
    c-profile))


 (defn profile-string [column c-profile]
   (update-in c-profile [:string :max_length] max (count column)))

 (defn profile-date [column c-profile]
   (if (isDate? column)
     (update-in c-profile [:date :count] + 1) c-profile))

 (defn profile-column [c-profile column]
   (->>
     c-profile
    (profile-missing column)
    (profile-integer column)
    (profile-string column)
    (profile-date column)
    ))


 (defn profile-row [profile row]
   (map profile-column profile row))

  ;;should probably get a distribution of row size and take the most common...note that hcat client just takes the max
 (defn get-num-columns [rows]
   (count (first rows)))


  (defn profile-data [rows]
    (reduce profile-row (repeat 23 a) rows))

  (defn profile-rdd-l [rdd]
   (->>
    rdd
    (spark/sample true 0.01 78)
    (spark/collect)
    (map #(first (csv/parse-csv %)))
    (profile-data)
    ))


