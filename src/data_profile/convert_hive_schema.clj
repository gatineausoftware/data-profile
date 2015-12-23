(ns data-profile.convert-hive-schema
  (:require   [clojure.string :as string]
              [clojure.math.numeric-tower :as m]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [clojure.java.io :as io]
              [clj-http.client :as client]
              [clojure.data.json :as json])
  (:use       [data-profile.util]))


(def min-int -2147483648)
(def max-int 2147483647)



(defn hiveDecimal? [a]
  (some? (re-find #"decimal" (str a))))


(defn getHiveDecimal [a]
  (let [b (re-find #"\(\d+,\d+\)" (a "type"))
        c (apply str (remove #((set "()") %) b))
        d (str/split c #",")
        digits (getInteger (first d))
        scale (getInteger (second d))
        m (dec (m/expt 10 (inc (- digits scale))))]

    {
     :name (keyword (a "name"))
     :type :decimal
     :max m
     :min (- m)
     :max_scale scale}))


(defn hiveInteger? [a]
  (= "integer" (a "type")))


(defn getHiveInteger [a]
  {
   :name (keyword (a "name"))
   :type :integer
   :min min-int
   :max max-int
  }
  )


;;generic catch all for types witout properties...
;;need to add smallint, float, double etc...
(defn getHiveDataType [a]
  {
   :name (keyword (a "name"))
   :type (keyword (a "type"))
   })



(defn convert-schema [a]
    (cond
      (hiveDecimal? a) (getHiveDecimal a)
      (hiveInteger? a) (getHiveInteger a)
      :else (getHiveDataType a)))


(defn get-schema [hcatserver port database table user]
  (->>
    (str "http://" hcatserver ":" port "/templeton/v1/ddl/database/" database "/table/" table "?user.name=" user)
    (client/get)
    (:body)
    (json/read-str)
    (#(% "columns"))
    (map convert-schema)
    (filter #(nil? (#{:mo :yr :dy} (% :name))))
    (vec)
   ))



(get-schema  "54.173.182.186" 50111 "crest_ccna_rstr_basc" "raw_t_fact_food_summary" "ec2-user")






