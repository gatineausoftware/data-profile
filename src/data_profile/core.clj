(ns data-profile.core
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.profile :as profile]
              [data-profile.cleanse-data :as cleanse]
              [data-profile.validate-hcat-schema :as hcat]
              [data-profile.validate-partitions :as part]
              [clojure.tools.cli :refer [parse-opts]])
  (:use       [data-profile.util]
              [data-profile.validate-schema]
              )
  (:gen-class))

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "data-profile"))]
    (spark/spark-context c)))


(def cli-options
  ;; An option with a required argument
  [["-d" "--delimiter d" "CSV delimiter"
    :default \,
    :parse-fn #(first (seq  %))]

    ["-s" "--sample s" "Sample Size"
    :default 1
    :parse-fn bigdec]

    ["-n" "--num-records n" "number of records"
    :default 10
    :parse-fn bigint]

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
        "  check-schema <filename> <schema> checks schema"
        "  cleanse <filename> <schema> <output> Writes file in to 'good' and 'bad' subdirectories"
        ""]
       (string/join \newline)))


(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))



;;this is a mess...need to check for proper number arguments differently...put into a dispatch map or vector?

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
      "count" (profile/count-num-records (spark/text-file sc (second arguments)))
      "profile" (profile/profile (spark/text-file sc (second arguments)) options)
      "list-schema-errors"
      (if
       (< (count arguments) 3)
            (exit 1 (usage summary))
            (list-schema-errors (spark/text-file sc (second arguments)) (nth arguments 2) options))
      "list-bad-records"
      (if
       (< (count arguments) 3)
            (exit 1 (usage summary))
            (list-bad-records(spark/text-file sc (second arguments)) (nth arguments 2) options))
      "cleanse"
       (if
       (< (count arguments) 4)
            (exit 1  (usage summary))
            (cleanse/cleanse (spark/text-file sc (second arguments)) (nth arguments 2) (nth arguments 3) options))
      "list-bad-records-hcat" (hcat/list-bad-records-hcat (spark/text-file sc (second arguments)) "hcatserver" "database" "table" options)
      "validate-partitions" (part/validate-partitions sc (second arguments))
      )
     (clojure.pprint/pprint))))









