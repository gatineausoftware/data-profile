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



;;get server, port and username from config files, as in validate_schema

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



  (defn count-records [sc database table directory]
      (let [rdd (spark/text-file sc (str directory "/"))
         c (spark/count rdd)]
      (println (str  database "." table "=>" directory ": " c))
      [database table directory c]))

  (defn count-records-for-table-partitions [sc database table]
    (let [partitions (get-partitions database table)]
      (map (partial count-records sc database table) partitions)))



  (defn get-tables-for-database [database]
     ((json/read-str ((client/get (str base "/database/" database "/table" "?user.name=ec2-user")) :body)) "tables"))


  (defn validate-some-partitions [sc database prefix]
    (let [
          tables (filter  #(= (str (first %)) prefix) (get-tables-for-database database))]
      (map (partial count-records-for-table-partitions sc database) tables)))

  (defn validate-partitions [sc database]
    (let [tables (get-tables-for-database database)]
      (map (partial count-records-for-table-partitions sc database) tables)))





