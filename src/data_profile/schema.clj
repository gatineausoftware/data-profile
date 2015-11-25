(ns data-profile.schema)

(def smallest_int -2147483648)
(def largest_int 2147483647)

 (def ahold-schema
   [{:name :week_id :type :integer :min 0 :max 999999}
    {:name :store_id :type :numeric}
    {:name :upc_id :type :numeric}
    {:name :household_id :type :numeric}
    {:name :facts :type :string}
    {:name :truprice :type :string}
    {:name :shopstyles :type :string}
    {:name :sales :type :float  :min -1000 :max 1000}
    {:name :transactions :type :integer :min 0 :max largest_int}
    {:name :units :type :integer :min -100 :max 100}
    ])


 (def redshift-schema
   [{:name :brand :type :varchar :size 100}
	  {:name :query_date :type :date}
	  {:name :documentID :type :varchar :size 50}
	  {:name :hashtags :type :varchar :size 512}
	  {:name :authorAvatarUrl :type :varchar :size 512}
	  {:name :language :type :varchar :size 50}
	  {:name :originalAuthorId :type :varchar :size 100}
	  {:name :authorId :type :varchar :size 100}
	  {:name :originalAuthor :type :varchar :size 100}
	  {:name :title :type :varchar :size 2048}
	  {:name :date_time :type :varchar :size 30}
	  {:name :dayofweek :type :varchar :size 30}
	  {:name :authorLink :type :varchar :size 512}
	  {:name :alertTimestamp :type :varchar :size 30}
	  {:name :klout :type :varchar :size 512}
	  {:name :time_stamp :type :varchar :size 30}
	  {:name :postType :type :varchar :size 30}
	  {:name :author :type :varchar :size 100}
	  {:name :authorGeoInfo :type :varchar :size 512}
	  {:name :docID :type :varchar :size 1024}
	  {:name :authorSex :type :varchar :size 10}
	  {:name :url :type :varchar :size 512}
	  {:name :authorFollower :type :varchar :size 512}
	  {:name :sourceType :type :varchar :size 100}
	  {:name :authorName :type :varchar :size 100}
	  {:name :domain :type :varchar :size 100}
	  {:name :hourOfDay :type :varchar :size 30}
    {:name :alertDatetime :type :varchar :size 512}
	  {:name :originalAuthorAvatarUrl :type :varchar :size 512}
    ])


 (defn get-schema [name]
  redshift-schema)


