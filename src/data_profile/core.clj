(ns data-profile.core
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema]
              [data-profile.profile :as profile]
              [data-profile.cleanse-data :as cleanse])
  (:use       [data-profile.util]
              [data-profile.schemavalidation]
              )
  (:gen-class))




(defn -main
  [command filename & args]
  (let [sc (make-spark-context)
        rdd (spark/text-file sc filename)]
  (->>
  (case command
    "count" (count-num-records rdd)
    "max-col-val" (max-col-val rdd (bigint (first args)))
    "max-col-count" (get-max-columns rdd)
    "min-col-count" (get-min-columns rdd)
    "count-incomplete-rows" (count-incomplete-records rdd (bigint (first args)))
    "get-incomplete-records" (get-incomplete-records rdd (bigint (first args)))
    "get-num-col-dist" (get-num-col-dist rdd)
    "write-bad-data" (write-bad-data rdd (schema/get-schema (first args)) (second args))
    "get-schema-errors" (get-schema-errors rdd (schema/get-schema (first args)))
    "check-schema" (check-schema rdd (schema/get-schema (first args)))
    "profile" (profile/profile 1 rdd)
    "cleanse" (cleanse/cleanse rdd (schema/get-schema (first args)) (second args))
    "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]")
   clojure.pprint/pprint)))








