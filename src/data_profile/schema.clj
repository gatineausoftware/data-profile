(ns data-profile.schema)



 ;reading schema in from file has caused me to loose the max int contant...how to do this properly
 (defn get-schema [name]
  (read-string (slurp name)))


