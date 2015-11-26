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
   (if (is-integer? field)
     (assoc-in p [:profile :integer] {:count 1 :min (string->integer field) :max (string->integer field)}) p)))


(defn profile-string [p]
   (let [field (:field p)]
   (assoc-in p [:profile :string :max_length] (count field))))



 (defn profile-field [field]
   (->
    {:field field :profile a}
    profile-integer
    profile-string
    ))


 ;row will be a java arraylist..
 (defn profile_row [row]
   (mapv profile-field row))


 (defn a_field [ap p]
   (->
    ap
    (update-in [:profile :integer :count] + (get-in p [:profile :integer :count]))
    (update-in [:profile :integer :max]  max (get-in p [:profile :integer :max]))
    (update-in [:profile :integer :min] min (get-in p [:profile :integer :min]))
    (update-in [:profile :string :max_length] max (get-in p [:profile :string :max_length]))))



 (defn a_row [ar r]
   (map a_field ar r))



 ;;stack blow out when running lcoally
 (defn profile-rdd [rdd]
   (->>
    rdd
    (spark/map #(first (csv/parse-csv %)))
    (spark/map profile_row)
    (spark/reduce a_row)
    ))




