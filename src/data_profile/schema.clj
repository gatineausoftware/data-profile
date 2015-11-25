(ns data-profile.schema
  (:require [clojure.java.io :as io]))



 ;reading schema in from file has caused me to loose the max int contant...how to do this properly
 ;assume schema is in resources directory
 (defn get-schema [name]
  (read-string (slurp (io/resource name))))


