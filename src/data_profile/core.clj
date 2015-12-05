(ns data-profile.core
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema]
              [data-profile.profile :as profile]
              [data-profile.cleanse-data :as cleanse]
              [clojure.tools.cli :refer [parse-opts]])
  (:use       [data-profile.util]
              [data-profile.schemavalidation]
              )
  (:gen-class))



(def cli-options
  ;; An option with a required argument
  [["-d" "--delimiter D" "CSV delimiter"
    :default \,
    :parse-fn #(first (seq  %))]

    ["-s" "--sample S" "Sample Size"
    :default 1
    :parse-fn bigdec]

    ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["utility for profiling data sets and validating schema"
        ""
        "Usage: action [options] <filename> [<schema>] ["
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "  count <filename> Count the number of lines in a data set"
        "  profile <filename> Profile contents of a dataset"
        "  check-schema <filename> <schema> checks a single row against a schema"
        ""]
       (string/join \newline)))


(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))


(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)
        sc (make-spark-context)
        ]
    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      (< (count arguments) 2) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (->>
    (case (first arguments)
      "count" (count-num-records (spark/text-file sc (second arguments)))
      "profile" (profile/profile-with-options (spark/text-file sc (second arguments)) options)
      "check-schema"
      (if
       (< (count arguments) 3)
            (exit 1 (usage summary))
            (check-schema (spark/text-file sc (second arguments)) (schema/get-schema (nth arguments 2)))))
     (clojure.pprint/pprint))))

(defn -kmain
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
    "profile" (profile/profile rdd args)
    "cleanse" (cleanse/cleanse rdd (schema/get-schema (first args)) (second args))
    "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]")
   clojure.pprint/pprint)))








