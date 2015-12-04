(ns data-profile.util-test
  (:use clojure.test)
  (:require [data-profile.util :as u]
))



(deftest utilunittest
  (testing
    (is (some? (u/isDate? "2015/10/31"))))
  (testing
    (is (some? (u/isDate? "10/31/15"))))

  )

