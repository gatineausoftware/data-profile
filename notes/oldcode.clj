(ns data-profile.profile
  (:require   [clojure.string :as string]
              [sparkling.conf :as conf]
              [sparkling.core :as spark]
              [sparkling.destructuring :as s-de]
              [clojure.string :as str]
              [clojure-csv.core :as csv]
              [data-profile.schema :as schema])
  (:use       [data-profile.util]))




(def a {:date {:count 0 :min -1 :max 1} :integer {:count 0 :min 0 :max 0} :string {:max_length 0}})


(defn profile-integer [p]
  (let [field (:field p)]
   (if (isInteger? field)
     (assoc-in p [:profile :integer] {:count 1 :min (getInteger field) :max (getInteger field)}) p)))


(defn profile-string [p]
   (let [field (:field p)]
   (assoc-in p [:profile :string :max_length] (count field))))



 (defn profile-field [field]
   (->
    {:field field :profile a}
    profile-integer
    profile-string
    ))



 (defn profile_row [row]
   (mapv profile-field row))


 (defn a_field [ap p]
   (->
    ap
    (update-in [:profile :integer :count] + (get-in p [:profile :integer :count]))
    (update-in [:profile :integer :max]  max (get-in p [:profile :integer :max]))
    (update-in [:profile :integer :min] min (get-in p [:profile :integer :min]))
    (update-in [:profile :string :max_length] max (get-in p [:profile :string :max_length]))))


 ;;this is causing the stack overflow...something to do with lazy sequences and thunking?
 (defn a_row [ar r]
   (map a_field ar r))


 ;;try with loop instead of map...this seems to work.
 ;;note that the profile will be truncated to smallest row (i.e., if there is incomplete data)
 (defn a_row_e [ar r]
   (loop [cp ar c r res []]
     ;(clojure.pprint/pprint cp)
     (if cp (recur (next cp) (next c) (conj res (a_field (first cp) (first c)))) res)))


 ;;note that this causes a stack overflow if i reduce with a_row.  The fact that functions passed to spark/reduce
 ;;need to be commutative make this a lot more complicated than if doing it locally.  This might be a better approach.
 ;;take a sample of the data, bring it back to driver and then operate locally.   performance appears to be
 ;;quite bad.  causes heap erorr on large data sets.

 (defn profile-rdd [rdd]
   (->>
    rdd
    (spark/map #(first (csv/parse-csv %)))
    (spark/map profile_row)
    (spark/reduce a_row_e)
    ;(spark/collect)
    ))


;;bit of nonesense to handle key-value pairs
 (defn get-num-col-dist [rdd]
    (->>
     rdd
    (spark/map #(str/split % #","))
    (spark/map-to-pair (fn [r] (spark/tuple (count r) 1)))
    (spark/reduce-by-key +)
    (spark/map (s-de/key-value-fn (fn [k v] [k v])))
     spark/collect))



;;will dorun fix stackoverflow problem?  maybe...but do run is only good
 ;;for side effects....returns nil
 ;(defn profile-row [num_columns profile row]
  ; (if (= (count row) num_columns)
     ;(dorun (map profile-column profile row)) profile))

;;stack overflow error becaues of lazy evaluation if i use a map here
; (defn profile-row [num_columns profile row]
 ;  (if (not= (count row) num_columns) profile
  ;    (loop [cp profile c row res []]
   ;    (if cp (recur (next cp) (next c) (conj res (profile-column (first cp) (first c)))) res))))

 (defn check-schema [rdd schema]
   (->>
     rdd
    (spark/map #(first (csv/parse-csv %)))
    (spark/take 1)
    first
    (map validate-field schema)))



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
