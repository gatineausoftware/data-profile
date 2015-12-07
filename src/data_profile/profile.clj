(ns data-profile.profile
  (:require   [clojure.string :as string]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema])
  (:use       [data-profile.util]))


(defn count-num-records [rdd]
   (->> rdd
         spark/count))


 (def a {:count 0 :missing 0 :max_length 0
         :numeric {:count 0 :integer_count 0  :min :none :max :none :min_scale 0 :max_scale 0}
         :date {:count 0 :earliest 0 :latest 0}})


 ;;(count column) is counting size of column...if it is zero, it's missing
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

;;ignoring incomplete rows...maybe extend profile to include
;;information about incomplete rows?
(defn profile-row [num_columns profile row]
  (if (= (count row) num_columns)
    (doall (map profile-column profile row)) profile))

  ;;take max (like hcat client) or take mode?
 (defn get-num-columns [rows]
   (->>
     rows
    (map count)
    (reduce max)))

  (defn profile-data [rows]
    (let [num_columns (get-num-columns rows)]
    (reduce (partial profile-row num_columns) (repeat num_columns a) rows)))


  (defn profile [rdd {:keys [delimiter sample]}]
   (->>
    rdd
    (spark/sample true sample 78)
    (spark/collect)
    (map #(first (csv/parse-csv % :delimiter delimiter)))
    (profile-data)
    ))


