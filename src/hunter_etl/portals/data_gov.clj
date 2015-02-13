(ns hunter-etl.portals.data-gov
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.util :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def dg-url "https://catalog.data.gov/api")

(defn dg-extract
  "extract data from the data.gov.uk API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan dg-url args))

;;;; transform

(defn get-created
  "try to get the created dataset date"
  [resource revision]
  (if-not (nil? resource)
    resource
    revision))

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
                 :tags (vec (concat (tagify-title (% :title))
                                    (extend-tags (get-tags (% :tags)))))
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

;;;; load

(defn dg-etl
  "data.gov ETL
  takes between 0 and 3 arguments :
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an offset
  3 => same but only with datasets corresponding to a query"
  [& args]
  (-> (apply dg-extract args)
      dg-transform
      load-to-hunter-api ; TODO: checker si ça load bien en base
      ))
