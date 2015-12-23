
(ns data-profile.convert-hive-schema-test
  (:use [clojure.test])
  (:require [data-profile.convert-hive-schema :as schema]
            [sparkling.core :as spark]
            [sparkling.conf :as conf]))



(def t [{:type :string, :name :period_cd}
        {:type :string, :name :geo_cd}
        {:type :string, :name :scc_cd}
        {:type :string, :name :menu_cd}
        {:type :string, :name :demo_cd}
        {:type :string, :name :daypart_cd}
        {:type :string, :name :whr_ord_cd}
        {:type (keyword "decimal(10,2)") :name :servings}
        {:type (keyword "decimal(10,2)") :name :servings_pcya}
        {:type (keyword "decimal(10,2)") :name :mnu_imprtnc}
        {:type (keyword "decimal(10,2)") :name :mnu_imprtnc_ptchg}
        ])



(deftest schematest1
  (testing
    (is (= true (schema/hiveDecimal? (keyword "decimal(10,2)")))))
  (testing
    (is (= {:name :a :type :decimal :max 999999 :min -999999 :max_scale 3} (schema/getHiveDecimal {"type" "decimal(9,3)" "name" "a"}))))

)


