(ns hunter-etl.portals.data-gouv-fr
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.transform :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dgf-url
  "data.gouv.fr API url: http://www.data.gouv.fr/api"
  "http://www.data.gouv.fr/api")

(defn dgf-extract
  "extract data from the data.gouv.fr API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan-v1 dgf-url args))

;;;; transform

(defn get-publisher
  "get the publisher and returns data.gouv.fr if it isn't available"
  [publisher]
  (if-not (nil? publisher)
    publisher
    "data.gouv.fr"))

(defn get-created
  "try to get the created dataset date"
  [resource alt]
  (if-not (nil? resource)
    resource
    alt))

(defn get-spatial
  "parses a collection of territories"
  [territories]
  (vec (mapcat geo-tagify
               (vec (map #(st/lower-case (get-in % [:name])) territories)))))

(defn clean-spatial
  "returns the geographic coverage if available"
  [geo]
  (if-not (empty? (get-spatial geo)) (get-spatial geo) (geo-tagify "france")))

(defn get-temporal
  "returns, if available the temporal coverage
  If it's not possible it try to find the temporal
  coverage from the resources"
  [from to]
  (if (and (not-empty from)
           (not-empty to))
    (extend-temporal (str from "/" to))
    "all"))

(defn filter-resources
  "returns a vector of resources limited to format, url and title"
  [coll]
  (vec (map #(select-keys % [:title :format :url]) coll)))

(defn dgf-huntscore
  "adapts the calculate-huntscore function to data.gouv.fr data"
  [reuses recent followers]
  (calculate-huntscore reuses recent 0 followers))

(deftransform dgf-transform
  [:title :page :description :last_modified :organization
   :spatial :tags :temporal_coverage :resources :metrics]

  {}
  
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
   :huntscore   [dgf-huntscore [:metrics :reuses] [:metrics :views] [:metrics :followers]]})

