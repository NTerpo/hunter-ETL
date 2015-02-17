(ns hunter-etl.transform
  (:use [clojure.data :refer :all])
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]))

;;;; Extract

(defn get-result
  "gets metadata from an API and provides a first basic filter"
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

;;;; Transform

(def hunter-keys
  "keys needed in the Hunter API
  [:title :description :publisher
  :uri :created :updated :spatial
  :temporal :tags :resources :huntscore]"
  [:title :description :publisher :uri :created :updated :spatial :temporal :tags :resources :huntscore])

(defn not-hunter-keys
  "keys on a collection that are not on the hunter-keys"
  [vect]
  (vec
   (second
    (diff (set hunter-keys) (set vect)))))

(defn geo-tagify
  "extend the spatial coverage tagging 'us'->'america'->'countries'->'world'"
  [geo]
  (let [geo (clojure.string/lower-case geo)]
    (if-not (nil? (some #{geo} ["france" "us" "europe" "world" "uk"]))
      ({"france" ["france" "fr" "europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "us" ["us" "usa" "america" "united states" "united-states" "united states of america" "united-states-of-america" "world" "countries" "all"]
        "europe" ["europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "world" ["world" "all" "countries"]
        "uk" ["uk" "england" "scotland" "wales" "ireland" "great-britain" "gb"]} geo)
      (vector geo))))

(defn extend-tags
  "create new tags with the given tags vector by spliting words and cleaning"
  [tags]
  (->> (vec (disj (set (->> (map st/lower-case tags)
                            (map st/trim)
                            (mapcat #(st/split % #"-"))
                            (concat tags))) "report" "data" "-" "service" "government"))
       (map #(st/replace % "," ""))
       (map #(st/replace % "(" ""))
       (map #(st/replace % ")" ""))))

(defn tagify-title
  "create new tags from the title"
  [title]
  (vec (disj (set (-> (st/lower-case title)
                      (st/split #" "))) "database" "-" "db" "data" "dataset" "to" "and")))

(defn extend-temporal
  "extend the temporal coverage with dates between limits"
  [temporal]
  (let [limits (re-seq #"[0-9]{4}" temporal)]
    (if (nil? limits)
      "all"
      (if (= 1 (count limits))
                        (vec limits)
                        (vec
                         (map str
                              (range (Integer. (first limits))
                                     (+ 1 (Integer. (last limits))))))))))

(defn calculate-huntscore
  "returns the sum of reuses, views/1000 recent-views/200 and followers/10"
  ([reuses] (calculate-huntscore reuses 0 0 0))
  ([reuses recent] (calculate-huntscore reuses recent 0 0))
  ([reuses recent views] (calculate-huntscore reuses recent views 0))
  ([reuses recent views followers]
   (reduce + [(* 1 reuses)
              (* 0.005 recent)
              (* 0.001 views)
              (* 0.1 followers)])))

;;;; Load

(def api-url
  "URL of the Hunter API
  used to POST the datasets after the transformations"
  "http://localhost:3000/api/datasets")

(defn load-to-hunter-api
  "Send each dataset to the Hunter API via method post"
  [coll]
  (map #(client/post api-url
                     {:body (generate-string %)
                      :content-type "application/json"}) coll))

;;;; By Hand Datasets

(defn create-ds
  "create a hunter dataset by hand
  eg. (create-ds \"Global Bilateral Migration Database\"
  \"Global matrices of bilateral migrant stocks spanning
  the period 1960-2000, disaggregated by gender and based
  primarily on the foreign-born concept are presented.
  Over one thousand census and population register records
  are combined to construct decennial matrices corresponding
  to the last five completed census rounds.\\n\\nFor the
  first time, a comprehensive picture of bilateral global
  migration over the last half of the twentieth century
  emerges. The data reveal that the global migrant stock
  increased from 92 to 165 million between 1960 and 2000
  . South-North migration is the fastest growing component
  of international migration in both absolute and relative
  terms. The United States remains the most important migrant
  destination in the world, home to one fifth of the world’s
  migrants and the top destination for migrants from no less
  than sixty sending countries. Migration to Western Europe
  remains largely from elsewhere in Europe. The oil-rich
  Persian Gulf countries emerge as important destinations
  for migrants from the Middle East, North Africa and
  South and South-East Asia. Finally, although the global
  migrant stock is still predominantly male, the proportion
  of women increased noticeably between 1960 and 2000.\"
  \"World Bank\" \"http://databank.worldbank.org/data/views/
  variableselection/selectvariables.aspx?source=global-bilateral
  -migration\" [\"World\" \"East Asia & Pacific\" \"Europe\" \"
  Asia\" \"Latin America\" \"Caribbean\" \"Middle East\" \"North
  Africa\" \"South Asia\" \"Sub Saharan\" \"Africa\"] \"1960 - 2000\"
  [\"migrants\" \"immigration\"] \"2011-07-01T00:00:00.000000\"
  \"2011-07-01T00:00:00.000000\" [] 5 0)"
  [title description publisher uri spatial temporal tags created updated resources reuses views]
  (list {:title title
         :description description
         :publisher publisher
         :uri uri
         :spatial spatial
         :temporal (extend-temporal temporal)
         :tags (vec (concat (tagify-title title)
                            tags))
         :created created
         :updated updated
         :resources resources
         :huntscore (calculate-huntscore reuses views 0 0)}))

;;;; Transformation Macro

(defmacro map-get-in
  "Given a map and a vector of key [f :key1 :key2]
  returns (f (get-in map [:key1]) (get-in map [:key2]))

  Used in the deftransform macro to avoid redundancy"
  [m coll]
  (let [args# `(map #(get-in ~m (if (vector? %) % [%]))
                      (rest ~coll))]
      (list apply `(first ~coll) args#)))

(defmacro deftransform
  "Define a function that transform a collection of
  dataset's metadata to make it meet the Hunter API
  scheme.

  The first argument is the name of the returned function

  The seconde one is an array of the keys present in
  the given collection of hashmaps and needed

  Then it takes a list of pairs: :hunter-key (ƒ :old-key)
  each function returns the final value of each key

  e.g.
  (deftransform dgf-transform
  
  [:title :page :description :last_modified :organization
   :spatial :tags :temporal_coverage :resources :metrics]
  
  {:title       [identity :title]
   :description [notes->description :description :title]
   :publisher   [get-publisher [:organization :name]]
   :uri         [url->uri :page]
   :created     [get-created [:resources 0 :created_at] :last_modified]
   :updated     [identity :last_modified]
   :spatial     [clean-spatial [:spatial :territories]]
   :temporal    [get-temporal [:temporal_coverage :start] [:temporal_coverage :end]]
   :tags        [tags-with-title :title :tags]
   :resources   [filter-resources :resources]
   :huntscore   [str [:metrics :reuses] [:metrics :views]]})"
  [name keys fns-map]                   ; TODO: add booleans
  (cons 'defn
        `(~name "Pipeline to transform the collection received from the API
  and make it meet the Hunter API scheme."
                [coll#]
                (let [ks# ~keys 
                      nks# (not-hunter-keys ks#)
                      {title-t# :title description-t# :description
                       publisher-t# :publisher uri-t# :uri
                       created-t# :created updated-t# :updated
                       spatial-t# :spatial temporal-t# :temporal
                       tags-t# :tags resources-t# :resources
                       huntscore-t# :huntscore} ~fns-map]

                  (->> coll#
                       (map #(select-keys % ks#))
                       (map #(assoc %
                               :title (map-get-in % title-t#)
                               :description (map-get-in % description-t#)
                               :publisher (if (= 1 (count publisher-t#))
                                            (first publisher-t#)
                                            (map-get-in % publisher-t#))
                               :uri (map-get-in % uri-t#)
                               :created (map-get-in % created-t#)
                               :updated (map-get-in % updated-t#)
                               :spatial (if (= 1 (count spatial-t#))
                                          (first spatial-t#)
                                          (map-get-in % spatial-t#))
                               :temporal (map-get-in % temporal-t#)
                               :tags (map-get-in % tags-t#)
                               :resources (map-get-in % resources-t#)
                               :huntscore (map-get-in % huntscore-t#)))
                       (map #(apply dissoc % nks#)))))))


