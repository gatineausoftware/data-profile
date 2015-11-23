(ns data-profile.core
(:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.string :as str])

  (:gen-class))


(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "data-profile"))]
    (spark/spark-context c)))




(defn count-records [sc filename]
	(->>
	(spark/text-file sc filename)
	(spark/map (fn [l] 1))
	(spark/reduce +)
	clojure.pprint/pprint))



(defn print-file [sc filename]
	(->>
    (spark/text-file sc filename)
	  spark/collect
	  clojure.pprint/pprint))


(defn get-sample [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/sample true 10 7)
   (spark/collect)))




(defn get-column [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   (clojure.pprint/pprint)))


(defn get-average [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map first)
   (spark/map bigdec)
   (spark/reduce +)
   (clojure.pprint/pprint)))


  (defn get-max [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/map #(nth % n))
    (spark/map bigdec)
    (spark/reduce max)
    (print)))


(defn -main
  [filename n & args]
(let [sc (make-spark-context)]
(get-max sc filename (bigdec n))))




