(ns data-profile.profile-test
  (:use clojure.test)
  (:require [clojure.set]
            [sparkling.core :as s]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as sd]
            [data-profile.profile :as p]
            [data-profile.util :as u]
            [clojure-csv.core :as csv]
            ))


(deftest profiletest1
  (testing
    (is (=
         (->
          p/a
          (assoc-in [:date :count] 1))
          (p/profile-date "2015/10/31" p/a))))
  (testing
    (is (=
         (->
          p/a
          (assoc-in [:date :count] 1))
          (p/profile-date "2015-10-31" p/a))))


  (testing
    (is (=
         (->
          p/a
          (assoc-in [:integer :count] 1)
          (assoc-in [:integer :max] 1001))
          (p/profile-integer "1001" p/a))))

  (testing
    (is (=
         (->
          p/a
          (assoc-in [:string :max_length] 4))
          (p/profile-string "1001" p/a))))


  (testing
    (is (=
         (->
          p/a
          (assoc-in [:integer :count] 1)
          (assoc-in [:integer :max] 42)
          (assoc-in [:string :max_length] 2))
          (p/profile-column p/a "42"))))
  )


;;problems: "15,000" is not parsing as an int
;;is 1004N a problem?
;;not running decimal yet
(def testdata2
  [["1001" "atlanta" "2015-10-31" "2.01" "15,000"]
  ["1002" "chicago" "2015-01-19" "3.2"  ""]])

(def testresult2
   (list (->
         p/a
         (assoc-in [:string :max_length] 4)
         (assoc-in [:integer :count] 2)
         (assoc-in [:integer :max] 1002)
         (assoc-in [:decimal :count] 2)
         (assoc-in [:decimal :max_scale] 0))

        (->
         p/a
         (assoc-in [:string :max_length] 7))

        (->
         p/a
         (assoc-in [:string :max_length] 10)
         (assoc-in [:date :count] 2))

        (->
         p/a
         (assoc-in [:string :max_length] 4)
         (assoc-in [:decimal :count] 2)
         (assoc-in [:decimal :max_scale] 2)
         )

         (->
          p/a
         (assoc-in [:string :max_length] 6)
         (assoc-in [:integer :count] 1)
         (assoc-in [:integer :max] 15000)
         (assoc-in [:decimal :count] 2)
         (assoc-in [:decimal :max_scale] 0)
         (assoc-in [:missing] 1)
        )))




(deftest profiletest2
  (testing
    (is (=
         (p/profile-data testdata2)
         testresult2
        ))))




(deftest profiletest
  (s/with-context
    sc
    (-> (conf/spark-conf)
        (conf/set-sparkling-registrator)
        (conf/set "spark.kryo.registrationRequired" "true")
        (conf/master "local[*]")
        (conf/app-name "api-test"))
    (testing
      (is (= 10
      (->>
        (s/text-file sc "resources/sample.csv")
        (s/count)))))
    (testing
      (is (= 23
        (->>
        (s/text-file sc "resources/sample.csv")
        (p/profile 1)
        count
        ))))))

