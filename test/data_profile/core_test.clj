(ns data-profile.core-test
  (:use clojure.test)
  (:require [clojure.set]
            [sparkling.core :as s]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as sd]
            [data-profile.profile :as p]
            [data-profile.util :as u]
            [clojure-csv.core :as csv]
            ))





(deftest parsecsvtest
  (testing
    (is (= [["a" "b" "c"]] (csv/parse-csv "a,b,c"))))

  (testing
    (is (= "a,b,c\n" (csv/write-csv [["a" "b" "c"]]))))

  ;(testing
  ;  (is (= 10 (count (map (fn [x] (csv/write-csv (vector x))) testdata4)))))

  )







