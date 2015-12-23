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
          (assoc-in [:numeric :integer_count] 1))
          (p/profile-integer "1001" p/a))))


  (testing
    (is (=
         p/a
          (p/profile-date "42" p/a))))

  (testing
    (is (=
         (->
          p/a
          (assoc-in [:max_length] 4))
          (p/profile-string "1001" p/a))))


  (testing
    (is (=
         (->
          p/a
          (assoc-in [:numeric :count] 1)
          (assoc-in [:numeric :min] 2.13M)
          (assoc-in [:numeric :max] 2.13M)
          (assoc-in [:numeric :max_scale] 2))
          (p/profile-numeric "2.13" p/a))))

  (testing
    (is (=
        (p/profile-numeric "not a number" p/a))))

  (testing
    (is (=
         (->
          p/a
          (assoc-in [:count] 1)
          (assoc-in [:max_length] 2)
          (assoc-in [:numeric :count] 1)
          (assoc-in [:numeric :max] 42M)
          (assoc-in [:numeric :min] 42M)
          (assoc-in [:numeric :integer_count] 1))
          (p/profile-column p/a "42"))))

  )




(def testdata2
  [["1001" "atlanta" "2015-10-31" "2.01" "15000"]
  ["1002" "chicago" "2015-01-19" "3.2"  ""]])

(def testresult2
   (vector (->
         p/a
         (assoc-in [:count] 2)
         (assoc-in [:max_length] 4)
         (assoc-in [:numeric :integer_count] 2)
         (assoc-in [:numeric :max] 1002M)
         (assoc-in [:numeric :min] 1001M)
         (assoc-in [:numeric :count] 2))


        (->
         p/a
         (assoc-in [:count] 2)
         (assoc-in [:max_length] 7))

        (->
         p/a
         (assoc-in [:count] 2)
         (assoc-in [:max_length] 10)
         (assoc-in [:date :count] 2))

        (->
         p/a
         (assoc-in [:count] 2)
         (assoc-in [:max_length] 4)
         (assoc-in [:numeric :count] 2)
         (assoc-in [:numeric :max_scale] 2)
         (assoc-in [:numeric :max] 3.2M)
         (assoc-in [:numeric :min] 2.01M))

         (->
          p/a
         (assoc-in [:count] 1)
         (assoc-in [:missing] 1)
         (assoc-in [:max_length] 5)
         (assoc-in [:numeric :integer_count] 1)
         (assoc-in [:numeric :max] 15000M)
         (assoc-in [:numeric :min] 15000M)
         (assoc-in [:numeric :count] 1))

        ))




(deftest profiletest2
  (testing
    (is (=
         (p/profile-data testdata2)
         testresult2
        ))))


(def testdata3
  [["1001" "atlanta" "2015-10-31" "2.01" "15000"]
   ["1002" "chicago" "2015-01-19" "3.2"  ""]
   ["1000" "bad"]])

(deftest profiletest3
  (testing
    (is (=
         (p/profile-data testdata3)
         testresult2
        ))))



(deftest profiletest4
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
        (s/text-file sc "test/sample_data/sample.csv")
        (s/count)))))
    (testing
      (is (= 23
        (->
        (s/text-file sc "test/sample_data/sample.csv")

        (p/profile {:sample 1 :delimiter \,})
        count
        ))))))

