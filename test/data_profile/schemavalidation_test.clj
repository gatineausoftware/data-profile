

(ns data-profile.schemavalidation-test
  (:use clojure.test)
  (:require [data-profile.schemavalidation :as schema]
))


(deftest test1
  (testing
    (schema/get-schema ahold.schema)
