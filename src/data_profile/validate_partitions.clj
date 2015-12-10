(ns data-profile.validate-partitions
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



(def base "http://54.173.182.186:50111/templeton/v1/ddl")

(defn get-partition [database table partition]
  (let [ p1 (client/get (str base "/database/" database "/table/" table "/partition/" (partition "name") "?user.name=ec2-user"))
         p2 ((json/read-str (p1 :body)) "location")]
    p2))


(defn get-partitions [database table]
  (let [p1 (client/get (str base "/database/" database "/table/" table "/partition/" "?user.name=ec2-user"))
        p2 ((json/read-str (p1 :body)) "partitions")
        ]
    (map (partial get-partition database table) p2)))


 (defn get-all-partitions-for-database [database]
  (let [p1 (client/get (str base "/database/" database "/table" "?user.name=ec2-user"))
        p2((json/read-str (p1 :body)) "tables")]

    (map (partial get-partitions database) p2)))


 (defn count-records [sc directory]
   (let [rdd (spark/text-file sc (str directory "/"))]
     (clojure.pprint/pprint directory)
     (clojure.pprint/pprint (spark/count rdd))))



 (defn validate-partitions [sc database]
   (let [partitions (get-all-partitions-for-database database)]
     (map (partial count-records sc) (first partitions))))




