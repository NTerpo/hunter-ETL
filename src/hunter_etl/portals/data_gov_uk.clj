(ns hunter-etl.portals.data-gov-uk
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.transform :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dguk-url
  "data.gov.uk API url: https://data.gov.uk/api"
  "http://data.gov.uk/api")

(defn dguk-extract
  "extract data from the data.gov.uk API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan-v3 dguk-url args))

;;;; transform

;; transformation functions

(defn get-spatial
  "returns the geographic coverage if available"
  [geo]
  (if (not-empty geo) geo (geo-tagify "uk")))

(defn get-resource-temporal
  "returns, if available, the year included in the description of
  the first and last resources and extend them"
  [vect]
  (let [f (first vect)
        l (last vect)
        from (re-seq #"[0-9]{4}" (get-in f [:description]))
        to (re-seq #"[0-9]{4}" (get-in l [:description]))]
    (if (and (not-empty from)
             (not-empty to))
      (extend-temporal (str from "/" to))
      "all")))

(defn get-temporal
  "returns, if available the temporal coverage
  If it's not possible it try to find the temporal
  coverage from the resources"
  [from to resources]
  (if (and (not-empty from)
           (not-empty to))
    (extend-temporal (str from "/" to))
    (get-resource-temporal resources)))

;; booleans to filter unuseful datasets

(defn published?
  "checks the unpublished key to filter not relevant datasets"
  [m]
  (= "false" (get-in m [:unpublished])))

(defn resources?
  "checkes if there are resources"
  [m]
  (not (empty? (get-in m [:resources]))))

(defn published-and-resources?
  "combines published? and resources? booleans"
  [m]
  (and published? resources?))

(defn dguk-huntscore
  "calculate huntscore for data.gov.uk data"
  [recent views]
  (calculate-huntscore 5 recent views 0))

;; transform 

(deftransform dguk-transform
  [:title :notes :organization :resources :tracking_summary
   :temporal_coverage-to :metadata_created :metadata_modified
   :temporal_coverage-from :geographic_coverage :url :tags]

  {:filter published-and-resources?}
  
  {:title       [identity :title]
   :description [notes->description :notes :title]
   :publisher   [identity [:organization :title]]
   :uri         [url->uri :url]
   :created     [identity :metadata_created] 
   :updated     [identity :metadata_modified]
   :spatial     [get-spatial :geographic_coverage] 
   :temporal    [get-temporal :temporal_coverage_from :temporal_coverage_to :resources]
   :tags        [tags-with-title :title :tags]
   :resources   [clean-resources :resources :title]
   :huntscore   [dguk-huntscore [:tracking_summary :total] [:tracking_summary :recent]]})
