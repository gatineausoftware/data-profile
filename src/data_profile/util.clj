(ns data-profile.util
(:require [clojure.string :as string]
            [clojure.string :as str]
            [clj-time.core :as t]
            [clj-time.format :as f]))





;tried to include "MM/dd/yy" but it didn't work
(def date-parser (f/formatter (t/default-time-zone) "YYYY-MM-dd" "YYYY/MM/dd" "MM/dd/yyyy"))

;;note it is important that the try return nil, not false, because of some?
(defn isDate? [x]
  (some? (try (f/parse date-parser x)
    (catch Exception e nil))))


(defn isInteger? [x]
   (try (bigint x)
    (catch Exception e false)))

(defn  getInteger [s]
    (try
      (int (bigint s))
      (catch Exception e nil)))

(defn getDecimal [s]
  (try
    (bigdec s)
    (catch Exception e nil)))

;;this seems silly..is there not a better way?
 (defn pmin [a b]
   (cond
    (= a :none) b
    (= b :none) a
    :else (min a b)))

 (defn pmax [a b]
   (cond
    (= a :none) b
    (= b :none) a
    :else (max a b)))
