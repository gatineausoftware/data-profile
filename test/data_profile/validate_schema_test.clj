(ns data-profile.validate-schema-test
  (:use [clojure.test])
  (:require [data-profile.validate-schema :as schema]))



(def testschema1
  [
  {:name "a" :type :string}
  {:name "b" :type :varchar :size 10}
  {:name "c" :type :date}
  {:name "d" :type :integer :min 0 :max 1000}
  {:name "e" :type :numeric}
  {:name "f" :type :decimal :min 0 :max 1000 :max_scale 2}
  ])

(def testrow1 ["atlanta" "chicago" "2015-01-01" "10" "22" "2.2"])


(deftest schematest1
  (testing
    (is (true? (schema/column-satisfies-schema? {:type :string} "atlanta"))))
  (testing
    (is (true? (schema/column-satisfies-schema? {:type :integer :min 0 :max 1000} 10))))
  (testing
    (is (true? (schema/column-satisfies-schema? {:type :decimal :min 0 :max 1000 :max_scale 2} 2.2))))
  (testing
    (is (false? (schema/column-satisfies-schema? {:type :integer :min 0 :max 10} 20))))
  (testing
    (is (true?
         (schema/row-satisfies-schema? testschema1 testrow1)))))



(deftest schematest2
  (testing
    (is (= {:name "a" :value "100" :error :int_range} (schema/validate-field {:name "a" :type :integer
                                                                     :min 0 :max 10} "100")))))


