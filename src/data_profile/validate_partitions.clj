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

(def config (read-string (slurp (io/resource "config/hcat_config.edn"))))

(def base (str "http://" (config :server) ":" (config :port) "/templeton/v1/ddl"))

;(def base "http://54.173.182.186:50111/templeton/v1/ddl")


(defn get-partition-location [database table partition]
  (let [ p1 (client/get (str base "/database/" database "/table/" table "/partition/" (partition "name") "?user.name=" (config :user)))
         p2 ((json/read-str (p1 :body)) "location")]
    p2))


(defn get-partitions [database table]
  (let [p1 (client/get (str base "/database/" database "/table/" table "/partition/" "?user.name=" (config :user)))
        p2 ((json/read-str (p1 :body)) "partitions")
        ]
    (map (partial get-partition-location database table) p2)))


;;only works with text files...
 (defn count-records [directory]
      (let [rdd (spark/text-file sc (str directory "/"))
         c (spark/count rdd)]
      c))


 (defn count-records-for-partitioned-table [database table]
   (reduce + map count-records (get-partitions database table)))



 (defn count-records-for-non-partitioned-table [database table]
    (let [f ((json/read-str ((client/get (str base "/database/data_lakes_views/table/bev_dstr_user_raw" "?format=extended" "&user.name=" (config :user))) :body)) "location")]
      (count-records f)))




  (defn get-tables-for-database [database]
     ((json/read-str ((client/get (str base "/database/" database "/table" "?user.name=" (config :user))) :body)) "tables"))



  (defn isPartitioned? [database table]
     ((json/read-str ((client/get (str base "/database/" database "/table/" table  "?format=extended" "&user.name=" (config :user))) :body)) "partitioned"))


  (defn count-records-for-table [database table]
     (let [count (if (isPartitioned? database table)
       (count-records-for-partitioned-table database table)
       (count-records-for-non-partitioned-table database table))]

       [database table count]))



  ;only look at raw tables for now
  (defn check-record-counts [database prefix]
    (let [
          tables (filter #(re-matches (re-pattern (str "^" prefix ".*")) %) (get-tables-for-database database))
          ;base_tables (map #(str/replace % #"_raw" "") (filter #(re-matches #".*_raw" %) tables))
          raw_tables (filter #(re-matches #".*_raw" %) tables)
          ]
      (map (partial count-records-for-table database) raw_tables)))
      ; raw_tables))

