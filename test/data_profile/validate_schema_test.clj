(ns data-profile.validate-schema-test
  (:use [clojure.test])
  (:require [data-profile.validate-schema :as schema]
            [sparkling.core :as spark]
            [sparkling.conf :as conf]))



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




(deftest schematest3
  (spark/with-context
    sc
    (-> (conf/spark-conf)
        (conf/set-sparkling-registrator)
        (conf/set "spark.kryo.registrationRequired" "true")
        (conf/master "local[*]")
        (conf/app-name "api-test"))
    (testing
      (is (= 2
           (->
           (spark/text-file sc "resources/sample2.csv")
           (schema/list-bad-records "testschema2.schema" {:delimiter \, :num-records 10})
           (count)))))

    (testing
      (is (= 3
           (->
           (spark/text-file sc "resources/sample2.csv")
           (schema/list-schema-errors "testschema2.schema" {:delimiter \, :num-records 10})
           (count)))))

    ))

