(ns data-profile.core-test
  (:use clojure.test)
  (:require [clojure.set]
            [sparkling.core :as s]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as sd]
            [data-profile.localprofile :as lp]
            [data-profile.util :as u]
            [clojure-csv.core :as csv]
            ))


;;the csv library doesn not seem to work from light table repl...need to specify line delimeter?


(def testdata4
  [["548" "SOY MILK CASE" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "96318" "(AH)-NATURAL ORGANIC" "92807" "(AH)-NATURAL ORGANIC" "93969" "(AH)-NATURAL BEVERAGE" "94666" "(AH)-NTRL NON DAIRY MILK" "94667" "(AH)-NTRL NON DIARY MILK" "" "4/9/08" "200815" "0" "N"]
   ["610" "SODA VENDING MACH" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "10/8/07" "200741" "54.15" "Y"]
   ["753" "S&S LOOSE SODA" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "12/5/07" "200749" "0" "N"]
   ["817" "LOOSE SENS SODA 12 OZ" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "11/2/07" "200744" "0" "N"]
   ["819" "V SODA CAN 12 OZ" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "10/1/07" "200740" "0" "N"]
   ["820" "V SODA BOTTLE SGL 20 OZ" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95118" "(AH)-CSD SINGLE BTTL" "" "10/1/07" "200740" "0" "N"]
   ["821" "V SNGLE BOTTLE 20 OZ" "UPC" "UNDEFINED BRAND" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95118" "(AH)-CSD SINGLE BTTL" "" "1/12/08" "200802" "0" "N"]
   ["1910" "SLCT VENDNG SODA 12 OZ" "UPC" "UNDEFINED BRAND" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "5/8/08" "200819" "0" "N"]
   ["2097" "COKE VENDING CANS 12 OZ" "UPC" "UNDEFINED BRAND" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95119" "(AH)-CSD SINGLE CAN" "" "10/4/07" "200740" "0" "N"]
   ["2098" "COKE VNDNG BTTLE 20 OZ" "UPC" "UNDEFINED BRAND" "0" "TOTAL" "1" "(AH)-Non Perishable" "92733" "(AH)-DSD" "92790" "(AH)-DSD BEVERAGE" "56" "(AH)-CARBONATED SOFT DRINK" "95114" "(AH)-CSD SINGLES" "95118" "(AH)-CSD SINGLE BTTL" "" "10/5/07" "200740" "0" "N"]])

(deftest parsecsvtest
  (testing
    (is (= [["a" "b" "c"]] (csv/parse-csv "a,b,c"))))

  (testing
    (is (= "a,b,c\n" (csv/write-csv [["a" "b" "c"]]))))

  (testing
    (is (= 10 (count (map (fn [x] (csv/write-csv (vector x))) testdata4))))))


(deftest utilunittest
  (testing
    (is (some? (u/isDate? "2015/10/31"))))
  (testing
    (is (some? (u/isDate? "10/31/15"))))

  )


(deftest profileunittest
  (testing
    (is (=
         (->
          lp/a
          (assoc-in [:date :count] 1))
          (lp/profile-date "2015/10/31" lp/a))))
  (testing
    (is (=
         (->
          lp/a
          (assoc-in [:date :count] 1))
          (lp/profile-date "2015-10-31" lp/a))))


  (testing
    (is (=
         (->
          lp/a
          (assoc-in [:integer :count] 1)
          (assoc-in [:integer :max] 1001))
          (lp/profile-integer "1001" lp/a))))

  (testing
    (is (=
         (->
          lp/a
          (assoc-in [:string :max_length] 4))
          (lp/profile-string "1001" lp/a))))


  (testing
    (is (=
         (->
          lp/a
          (assoc-in [:integer :count] 1)
          (assoc-in [:integer :max] 42)
          (assoc-in [:string :max_length] 2))
          (lp/profile-column lp/a "42"))))
  )



(def row1 ["548" "SOY MILK CASE" "UPC" "CTL BR" "0" "TOTAL" "1" "(AH)-Non Perishable" "96318" "(AH)-NATURAL ORGANIC" "92807" "(AH)-NATURAL ORGANIC" "93969" "(AH)-NATURAL BEVERAGE" "94666" "(AH)-NTRL NON DAIRY MILK" "94667" "(AH)-NTRL NON DIARY MILK" "" "4/9/08" "200815" "0" "N"])



(def result1 '({:date {:min -1, :max 1, :count 0},
  :string {:max_length 4},
  :integer {:min 0, :max 2097N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 23},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 3},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 15},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 1},
  :integer {:min 0N, :max 0N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 1},
  :integer {:min 0, :max 1N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 19},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 96318N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 20},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 92807N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 20},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 93969N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 26},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 95114N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 24},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 5},
  :integer {:min 0, :max 95119N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 24},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 0},
  :integer {:min 0, :max 0, :count 0},
  :missing 10}
 {:date {:min -1, :max 1, :count 10},
  :string {:max_length 7},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 6},
  :integer {:min 0, :max 200819N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 1},
  :integer {:min 0N, :max 0N, :count 10},
  :missing 0}
 {:date {:min -1, :max 1, :count 0},
  :string {:max_length 1},
  :integer {:min 0, :max 0, :count 0},
  :missing 0}))

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
      (is (= result1
        (->>
        (s/text-file sc "resources/sample.csv")
        (lp/profile-rdd-l 1)
        ))))

    ))


(def test-data3
[[{:field "548", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 3}, :integer {:count 1, :min 548N, :max 548N}}}
 {:field "SOY MILK CASE", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 13}, :integer {:min 0, :max 0, :count 0}}}
 {:field "UPC", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 3}, :integer {:min 0, :max 0, :count 0}}}
 {:field "CTL BR", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 6}, :integer {:min 0, :max 0, :count 0}}}
 {:field "0", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 0N, :max 0N}}}
 {:field "TOTAL", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:min 0, :max 0, :count 0}}}
 {:field "1", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 1N, :max 1N}}}
 {:field "(AH)-Non Perishable", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 19}, :integer {:min 0, :max 0, :count 0}}}
 {:field "96318", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 96318N, :max 96318N}}}
 {:field "(AH)-NATURAL ORGANIC", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 20}, :integer {:min 0, :max 0, :count 0}}}
 {:field "92807", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 92807N, :max 92807N}}}
 {:field "(AH)-NATURAL ORGANIC", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 20}, :integer {:min 0, :max 0, :count 0}}}
 {:field "93969", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 93969N, :max 93969N}}}
 {:field "(AH)-NATURAL BEVERAGE", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 21}, :integer {:min 0, :max 0, :count 0}}}
 {:field "94666", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 94666N, :max 94666N}}}
 {:field "(AH)-NTRL NON DAIRY MILK", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 24}, :integer {:min 0, :max 0, :count 0}}}
 {:field "94667", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 94667N, :max 94667N}}}
 {:field "(AH)-NTRL NON DIARY MILK", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 24}, :integer {:min 0, :max 0, :count 0}}}
 {:field "", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 0}, :integer {:min 0, :max 0, :count 0}}}
 {:field "4/9/08", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 6}, :integer {:min 0, :max 0, :count 0}}}
 {:field "200815", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 6}, :integer {:count 1, :min 200815N, :max 200815N}}}
 {:field "0", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 0N, :max 0N}}}
 {:field "N", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:min 0, :max 0, :count 0}}}]

[{:field "821", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 3}, :integer {:count 1, :min 821N, :max 821N}}}
 {:field "V SNGLE BOTTLE 20 OZ", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 20}, :integer {:min 0, :max 0, :count 0}}}
 {:field "UPC", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 3}, :integer {:min 0, :max 0, :count 0}}}
 {:field "UNDEFINED BRAND", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 15}, :integer {:min 0, :max 0, :count 0}}}
 {:field "0", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 0N, :max 0N}}}
 {:field "TOTAL", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:min 0, :max 0, :count 0}}}
 {:field "1", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 1N, :max 1N}}}
 {:field "(AH)-Non Perishable", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 19}, :integer {:min 0, :max 0, :count 0}}}
 {:field "92733", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 92733N, :max 92733N}}}
 {:field "(AH)-DSD", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 8}, :integer {:min 0, :max 0, :count 0}}}
 {:field "92790", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 92790N, :max 92790N}}}
 {:field "(AH)-DSD BEVERAGE", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 17}, :integer {:min 0, :max 0, :count 0}}}
 {:field "56", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 2}, :integer {:count 1, :min 56N, :max 56N}}}
 {:field "(AH)-CARBONATED SOFT DRINK", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 26}, :integer {:min 0, :max 0, :count 0}}}
 {:field "95114", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 95114N, :max 95114N}}}
 {:field "(AH)-CSD SINGLES", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 16}, :integer {:min 0, :max 0, :count 0}}}
 {:field "95118", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 5}, :integer {:count 1, :min 95118N, :max 95118N}}}
 {:field "(AH)-CSD SINGLE BTTL", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 20}, :integer {:min 0, :max 0, :count 0}}}
 {:field "", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 0}, :integer {:min 0, :max 0, :count 0}}} \
 {:field "1/12/08", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 7}, :integer {:min 0, :max 0, :count 0}}}
 {:field "200802", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 6}, :integer {:count 1, :min 200802N, :max 200802N}}}
 {:field "0", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:count 1, :min 0N, :max 0N}}}
 {:field "N", :profile {:date {:min -1, :max 1, :count 0}, :string {:max_length 1}, :integer {:min 0, :max 0, :count 0}}}]])




