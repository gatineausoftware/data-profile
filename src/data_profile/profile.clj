(ns data-profile.profile
  (:require   [clojure.string :as string]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema])
  (:use       [data-profile.util]))



 (defn pmin [a b]
   (cond
    (= a :none) b
    (= b :none) a
    :else (min a b)))



 (defn pmax [a b]
   (cond
    (= a :none) b
    (= b :none) a
    :else (max a b)))


 (def a {:count 0 :missing 0 :max_length 0
         :numeric {:count 0 :integer_count 0  :min :none :max :none :min_scale 0 :max_scale 0}
         :date {:count 0 :earliest 0 :latest 0}})


 ;;count column is counting size of column...if it is zero, it's missing
 (defn profile-missing [column c-profile]
   (if (zero? (count column))
              (update-in c-profile [:missing] + 1) (update-in c-profile [:count] + 1)))

 (defn profile-string [column c-profile]
   (update-in c-profile [:max_length] max (count column)))


 (defn profile-numeric [column c-profile]
   (if-let [d (getDecimal column)]
     (->
      c-profile
      (update-in [:numeric :count] + 1)
      (update-in [:numeric :max_scale] max (.scale d))
      (update-in [:numeric :min_scale] min (.scale d))
      (update-in [:numeric :min] pmin d)
      (update-in [:numeric :max] pmax d)
      ) c-profile))


 (defn profile-integer [column c-profile]
  (if-let [int-col (getInteger column)]
   (->
      c-profile
     (update-in [:numeric :integer_count] + 1)
    )
    c-profile))


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
    (profile-numeric column)
    ))


 (defn profile-row [num_columns profile row]
   (if (not= (count row) num_columns) profile
      (loop [cp profile c row res []]
       (if cp (recur (next cp) (next c) (conj res (profile-column (first cp) (first c)))) res))))




  ;;should probably get a distribution of row size and take the most common...note that hcat client just takes the max
 (defn get-num-columns [rows]
   (count (first rows)))

  (defn profile-data [rows]
    (let [num_columns (get-num-columns rows)]
    (reduce (partial profile-row num_columns) (repeat num_columns a) rows)))



  (defn profile-with-options [rdd {:keys [delimiter sample]}]
   (->>
    rdd
    (spark/sample true sample 78)
    (spark/collect)
    (map #(first (csv/parse-csv % :delimiter delimiter)))
    (profile-data)
    ))



  ;;this is a mess....
  ;;need to convert string to char...
  (defn profile [rdd args]
    (let [opts
          (->
           {}
           (assoc-in [:sample] (bigdec (first args)))
           (assoc-in [:delimiter] (first (seq (second args)))))]

      (profile-with-options rdd (merge {:sample 1 :delimiter \,} opts))))




;;will dorun fix stackoverflow problem?  maybe...but do run is only good
 ;;for side effects....returns nil
 ;(defn profile-row [num_columns profile row]
  ; (if (= (count row) num_columns)
     ;(dorun (map profile-column profile row)) profile))

