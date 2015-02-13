(ns hunter-etl.portals.data-gouv-fr
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.transform :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dgf-url "http://www.data.gouv.fr/api")

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
  (if-not (empty? (get-spatial geo)) geo (geo-tagify "france")))

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

(defn dgf-transform
  "pipeline to transform the collection received from the API
  and make it meet the Hunter API scheme.

  Are needed the following keys:
  :title :description :publisher :uri :created :updated :spatial
  :temporal :tags :resources :huntscore

  First the collection is filtered with booleans
  Then the Hunter keys are created from existent keys
  And, finally, the other keys are removed"
  [coll]
  (let [ks [:title :page :description :last_modified :organization
            :spatial :tags :temporal_coverage :resources :metrics]
        nks (not-hunter-keys ks)]

    (->> coll
         (map #(select-keys % ks))
         (map #(assoc %
                 :description (notes->description (% :description) (% :title))
                 :publisher (get-publisher (get-in % [:organization :name]))
                 :uri (url->uri (% :page))
                 :created (get-created (get-in % [:resources 0 :created_at])
                                       (% :last_modified))
                 :updated (% :last_modified)
                 :spatial (clean-spatial (get-in % [:spatial :territories]))
                 :temporal (get-temporal (get-in % [:temporal_coverage :start])
                                         (get-in % [:temporal_coverage :end]))
                 :tags (tags-with-title (% :title) (% :tags))
                 :resources (filter-resources (% :resources))
                 :huntscore (calculate-huntscore
                             (get-in % [:metrics :reuses])
                             (get-in % [:metrics :views])
                             0
                             (get-in % [:metrics :followers]))))
         (map #(apply dissoc % nks)))))
