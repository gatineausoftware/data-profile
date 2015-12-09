(ns data-profile.validate-hcat-schema
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [clojure.java.io :as io]
              [clj-http.client :as client]
              [clojure.data.json :as json])
  (:use       [data-profile.util]))




(defn convert-ddl [c]
  (->
   {}
   (assoc-in [:name] (keyword (c "name")))
   (assoc-in [:type] (keyword (c "type")))))



(defn get-hcat-schema [hcatserver database table]
  ;;for now
  (->>
   (((json/read-str (client/get "http://54.173.182.186:50111/templeton/v1/ddl/database/ahold/table/ahold_raw?user.name=ec2-user")) :body) "columns")
   (map convert-ddl)
   (filter #(nil? (#{:mo :yr :dy} (% :name))))
   vec))



;;this duplicates from validate_schema...need to consolidate somehow.  ideally insert bounds on ints etc...

(defn valid-column? [a b]
   (case (:type a)
     :integer (if-let [i (getInteger b)]
                true
                false)
     :numeric (if-let [d (getDecimal b)]
                true false)
     :varchar (<= (count b) (:size a))
     :date (isDate? b)
     :decimal (if-let [d (getDecimal b)]
                true false)
     true))


 (defn valid-row? [schema row]
   (and (= (count schema) (count row))
     (every? true? (map valid-column? schema row))))



(defn list-bad-records-hcat [rdd hcatserver database table {:keys [delimiter num-records]}]
    (->>
     rdd
     (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
     (spark/filter (complement (partial valid-row? (get-hcat-schema hcatserver database table))))
     (spark/take num-records)))

