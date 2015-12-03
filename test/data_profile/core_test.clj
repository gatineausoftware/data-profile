(ns data-profile.core-test
  (:use clojure.test)
  (:require [clojure.set]
            [sparkling.core :as s]
            [sparkling.conf :as conf]
            [sparkling.destructuring :as sd]
            [data-profile.profile :as p]
            [data-profile.util :as u]
            [clojure-csv.core :as csv]
            ))






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







