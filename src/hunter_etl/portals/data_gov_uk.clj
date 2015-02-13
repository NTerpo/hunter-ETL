(ns hunter-etl.portals.data-gov-uk
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.util :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dguk-url "http://data.gov.uk/api")

(defn dguk-extract
  "extract data from the data.gov.uk API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan dguk-url args))

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

;; transform 

(defn dguk-transform
  "pipeline to transform the collection received from the API
  and make it meet the Hunter API scheme.

  Are needed the following keys:
  :title :description :publisher :uri :created :updated :spatial
  :temporal :tags :resources :huntscore

  First the collection is filtered with booleans
  Then the Hunter keys are created from existent keys
  And, finally, the other keys are removed"
  [coll]
  (let [ks [:title :notes :organization :resources :tracking_summary
            :temporal_coverage-to :metadata_created :metadata_modified
            :temporal_coverage-from :geographic_coverage :url :tags]
        nks (not-hunter-keys ks)]

    (->> coll
         (filter published?)
         (filter resources?)
         (map #(select-keys % ks))
         (map #(assoc %
                 :description (notes->description (% :notes) (% :title))
                 :publisher (get-in % [:organization :title])
                 :uri (url->uri (% :url))
                 :created (% :metadata_created) 
                 :updated (% :metadata_modified)
                 :spatial (get-spatial (% :geographic_coverage)) 
                 :temporal (get-temporal (% :temporal_coverage_from)
                                         (% :temporal_coverage_to)
                                         (% :resources))
                 :tags (vec (concat (tagify-title (% :title))
                                    (extend-tags (get-tags (% :tags)))))
                 :resources (clean-resources (% :resources) (% :title))
                 :huntscore (calculate-huntscore
                             5 
                             (get-in % [:tracking_summary :total])
                             (get-in % [:tracking_summary :recent])
                             0)))   ; TODO: find a way to give a score
         (map #(apply dissoc % nks)))))

;;;; load

(defn dguk-etl
  "data.gov.uk ETL
  takes between 0 and 3 arguments :
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an offset
  3 => same but only with datasets corresponding to a query"
  [& args]
  (-> (apply dguk-extract args)
      dguk-transform
      load-to-hunter-api))
