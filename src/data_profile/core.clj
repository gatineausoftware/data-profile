(ns data-profile.core
(:require [clojure.string :as string]
            [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.destructuring :as s-de]
            [clojure.string :as str])

  (:gen-class))


(def smallest_int -2147483648)
(def largest_int 2147483647)

(defn make-spark-context []
  (let [c (-> (conf/spark-conf)
              (conf/master "local[*]")
              (conf/app-name "data-profile"))]
    (spark/spark-context c)))



;;plow through exceptions
(defn  string->integer [s]
    (try
      (bigint s)
      (catch Exception e 0)))




 ;;this doesn't work.  returns the whole file
(defn get-sample [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/sample true 10 7)
   (spark/collect)))




(defn get-row [sc filename]
  (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/first)
   ))


;;skip incomplete records...or should i just filter first?
(defn getcolumn [n row]
  (let [s (str/split row #",")]
    (try
      (nth s n)
      (catch Exception e nil))))




(defn get-average [sc filename n]
  (let [d (spark/text-file sc filename)
        c (spark/count d)
        s (->>
            (spark/map (partial getcolumn n))
            (spark/map bigint)
            (spark/reduce +))
        ]
    (/ s c)))


(defn get-incomplete-records [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
     spark/collect
    ))


(defn count-incomplete-records [sc filename n]
   (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter #(< (count %) n))
    (spark/count)
    ))


;;only look at valid records
  (defn max-col-val [sc filename n]
  (->>
    (spark/text-file sc filename)
    (spark/map (partial getcolumn n))
    (spark/map string->integer)
    (spark/reduce max)))


 (defn count-num-records [sc filename]
   (->> (spark/text-file sc filename)
         spark/count
    ))


 (defn get-max-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce max)))



 ;;could change this to count distribution of number of columns
 (defn get-min-columns [sc filename]
   (->>
   (spark/text-file sc filename)
   (spark/map #(str/split % #","))
   (spark/map count)
   (spark/reduce min)
   ))


 ;;bit of nonesense to handle key-value pairs
 (defn get-num-col-dist [sc filename]
    (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/map-to-pair (fn [r] (spark/tuple (count r) 1)))
    (spark/reduce-by-key +)
    (spark/map (s-de/key-value-fn (fn [k v] [k v])))
     spark/collect))




 (def schema
   [{:name :week_id :type :integer :min 0 :max 999999}
    {:name :store_id :type :numeric}
    {:name :upc_id :type :numeric}
    {:name :household_id :type :numeric}
    {:name :facts :type :string}
    {:name :truprice :type :string}
    {:name :shopstyles :type :string}
    {:name :sales :type :float  :min 0 :max 100000}
    {:name :transactions :type :integer :min 0 :max largest_int}
    {:name :units :type :integer :min 0 :max largest_int}
    ])


 (defn check-field [a b]
   ;(clojure.pprint/pprint b)
   (case (:type a)
     :integer (if (try (integer? (bigint b))
                (catch Exception e false))
                (and (<= (bigint b) (:max a) ) (>= (bigint b) (:min a)))
                false)
     :numeric (if (try (integer? (bigint b))
                    (catch Exception e false))
                true)

     true))



 (defn check-row [schema row]
   (if (= (count schema) (count row))
     (every? true? (map check-field schema row))
     false))


 (defn check-schema [sc filename]
    (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter  (complement (partial check-row schema)))
    (spark/collect)))


 (defn write-bad-data [sc filename output]
   (->>
    (spark/text-file sc filename)
    (spark/map #(str/split % #","))
    (spark/filter  (complement (partial check-row schema)))
    (spark/map #(apply str (interpose "," %)))
    (spark/save-as-text-file output)))



(defn -main
  [command filename & args]
  (let [sc (make-spark-context)]
  (->>
  (case command
    "count" (count-num-records sc filename)
    "max-col-val" (max-col-val sc filename (bigint (first args)))
    "max-col-count" (get-max-columns sc filename)
    "min-col-count" (get-min-columns sc filename)
    "count-incomplete-rows" (count-incomplete-records sc filename (bigint (first args)))
    "get-incomplete-records" (get-incomplete-records sc filename (bigint (first args)))
    "get-num-col-dist" (get-num-col-dist sc filename)
    "check-schema" (check-schema sc filename)
    "write-bad-data" (write-bad-data sc filename (first args))
    "usage: [count, max-col-val, max-col-count, min-col-count, count-incomplete-rows] [n]")
   clojure.pprint/pprint)))








