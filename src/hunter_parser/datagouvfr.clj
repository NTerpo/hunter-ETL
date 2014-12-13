(ns hunter-parser.datagouvfr
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://www.data.gouv.fr/api/1/")
;; (def api-key "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoiNTM3MDBiYzFhM2E3Mjk0NjAwNDM1ZmJkIiwidGltZSI6MTQxNTYyODcyMi40NzQ3NzZ9.MLofKql5iK7JR1LdfQZfUxjYjA194i5gfIbIe0IZM1Q")

(defn get-spatial
  ""
  [territories]
  (vec (mapcat geo-tagify
               (vec (map #(st/lower-case (get-in % [:name])) territories)))))

(defn clean-resources
  ""
  [coll]
  (vec (map #(select-keys % [:title :format :url]) coll)))

(defn get-datagouvfr-ds
  "gets a number of the most popular datasets' metadata from the API of data.gouv.fr and transforms them to match the Hunter API scheme"
  [number]
  (let [response ((parse-string (:body (client/get (str base-url "datasets/?sort=-reuses"
                                                         "&page_size=" number))) true) :data)]
    (->> (map #(select-keys % [:title :page :description :last_modified :organization :spatial :tags :temporal_coverage :resources :metrics]) response)
         (map #(assoc %
                 :uri (% :page)
                 :publisher (if-not (nil? (get-in % [:organization :name]))
                              (get-in % [:organization :name])
                              "data.gouv.fr")
                 :spatial (if-not (empty? (get-spatial (get-in % [:spatial :territories])))
                            (get-spatial (get-in % [:spatial :territories]))
                            (geo-tagify "france"))
                 :tags (vec (concat (tagify-title (% :title))
                                    (extend-tags (% :tags))))
                 :description (if-not (empty? (% :description))
                                (% :description)
                                (% :title))
                 :temporal (if-not (empty? (% :temporal_coverage))
                             (extend-temporal (str (get-in % [:temporal_coverage :start])
                                                   "/"
                                                   (get-in % [:temporal_coverage :end])))
                             "all")
                 :created (if-not (nil? (get-in % [:resources 0 :created_at]))
                            (get-in % [:resources 0 :created_at])
                            (% :last_modified))
                 :updated (% :last_modified)
                 :resources (clean-resources (% :resources))
                 :huntscore (calculate-huntscore (get-in % [:metrics :reuses])
                                                 (get-in % [:metrics :views])
                                                 0
                                                 (get-in % [:metrics :followers]))))
         (map #(dissoc % :organization :temporal_coverage :page :metrics :last_modified)))))
