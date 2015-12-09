(ns data-profile.cleanse-data
  (:require   [clojure.string :as string]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.validate-schema :as c])
  (:use       [data-profile.util]
              ))

 ;;the write/csv is seems to be adding an additional cr...need to incoporate delimiter
 (defn write-good-rows [schema output rdd]
   (println output)
    (->>
     rdd
    (spark/filter  (partial c/valid-row? schema))
    (spark/map (fn [x] (csv/write-csv (vector x))))
    (spark/save-as-text-file (str output "/good"))))

 (defn write-bad-rows [schema output rdd]
    (->>
     rdd
    (spark/filter  (complement (partial c/valid-row? schema)))
    (spark/map (fn [x] (csv/write-csv (vector x))))
    (spark/save-as-text-file(str output "/bad"))))



(defn cleanse [rdd schema_name output {:keys [delimiter]}]
  (let [parsed-data
    (spark/map #(first (csv/parse-csv % :delimiter delimiter)) rdd)
        schema (c/get-schema schema_name)]

    (spark/cache parsed-data)
    (write-good-rows schema output parsed-data)
    (write-bad-rows schema output parsed-data)))

