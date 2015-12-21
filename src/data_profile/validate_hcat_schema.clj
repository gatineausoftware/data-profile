(ns data-profile.validate-hcat-schema
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


;(def base "http://54.173.182.186:50111/templeton/v1/ddl")


(defn convert-ddl [c]
  (->
   {}
   (assoc-in [:name] (keyword (c "name")))
   (assoc-in [:type] (keyword (c "type")))))




(defn hiveDecimal? [a]
  (some? (re-find #":decimal" (str a))))


(defn getHiveDecimal [a]
  (let [b (re-find #"\(\d+,\d+\)" (str (:type a)))
        c (apply str (remove #((set "()") %) b))
        d (str/split c #",")
        digits (getInteger (first d))
        scale (getInteger (second d))]

    {:type :decimal
     :max (m/expt 10 (- digits scale))
     :max_scale scale}))


(defn convert-schema [a]
    (cond
      (hiveDecimal? (:type a)) (getHiveDecimal a)
     :else
     a))


(defn get-hcat-schema [hcatserver port database table]
  (let [t (client/get (str "http://" hcatserver ":" port "/templeton/v1/ddl/database/" database "/table/" table "?user.name=ec2-user"))
        ddl (vec (filter #(nil? (#{:mo :yr :dy} (% :name))) (map convert-ddl ((json/read-str (t :body)) "columns"))))]
   (map convert-schema ddl)))




;;if column is missing it's ok....probably this should be specified.
(defn valid-column? [a b]
  (if (empty? b)
    true
   (case (:type a)
     :integer (if-let [i (getInteger b)]
                true
                false)
     :numeric (if-let [d (getDecimal b)]
                true false)
     :varchar (<= (count b) (:size a))
     :date (isDate? b)
     :decimal (if-let [d (getDecimal b)]
                (and (<= d (:max a)) (<= (.scale d) (:max_scale a))) false)

     true)))


 (defn valid-row? [schema row]
   (and (= (count schema) (count row))
     (every? true? (map valid-column? schema row))))


;;change this to get partitions from hcat, instead of passing in file?
(defn list-bad-records-hcat [rdd hcatserver database table {:keys [delimiter num-records port]}]
    (->>
     rdd
     (spark/map #(first (csv/parse-csv % :delimiter delimiter)))
     (spark/filter (complement (partial valid-row? (get-hcat-schema hcatserver port database table))))
     (spark/take num-records)))

