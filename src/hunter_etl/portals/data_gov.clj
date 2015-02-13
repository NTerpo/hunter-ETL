(ns hunter-etl.portals.data-gov
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.transform :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dg-url "https://catalog.data.gov/api")

(defn dg-extract
  "extract data from the data.gov API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan-v3 dg-url args))

;;;; transform

(defn get-created
  "try to get the created dataset date"
  [resource alt]
  (if-not (nil? resource)
    resource
    alt))

(defn clean-temporal
  "try to find the value of 'temporal' from a data.gov metadata dataset"
  [vect]
  (first
   (filter #(not (nil? %))
           (map #(get % :temporal)
                (map #(hash-map (keyword (% :key)) (% :value))
                     (map #(select-keys % [:key :value]) vect))))))

(defn get-temporal
  "if possible, returns the extended vector of the cleaned temporal"
  [extras]
  (if-not (nil? (clean-temporal extras))
    (extend-temporal (clean-temporal extras))
    "all"))

(defn dg-transform
  "pipeline to transform the collection received from the API
  and make it meet the Hunter API scheme.

  Are needed the following keys:
  :title :description :publisher :uri :created :updated :spatial
  :temporal :tags :resources :huntscore

  First the collection is filtered with booleans
  Then the Hunter keys are created from existent keys
  And, finally, the other keys are removed"
  [coll]
  (let [ks [:title :notes :organization :resources :tags :extras
            :revision_timestamp :tracking_summary]
        nks (not-hunter-keys ks)]
    (->> coll
         (map #(select-keys % ks))
         (map #(assoc %
                 :description (notes->description (% :notes) (% :title))
                 :publisher (get-in % [:organization :title])
                 :uri (url->uri (get-in % [:resources 0 :url]))
                 :created (get-created (get-in % [:resources 0 :created])
                                       (% :revision_timestamp))
                 :updated (% :revision_timestamp)
                 :tags (tags-with-title (% :title) (get-tags (% :tags)))
                 :spatial (geo-tagify "us") ; TODO: find the real
                                        ; spatial coverage
                 :temporal (get-temporal (% :extras))
                 :resources (clean-resources (% :resources) (% :title))
                 :huntscore (calculate-huntscore
                             0
                             (get-in % [:tracking_summary :total])
                             (get-in % [:tracking_summary :recent])
                             0)))
         (map #(apply dissoc % nks)))))
