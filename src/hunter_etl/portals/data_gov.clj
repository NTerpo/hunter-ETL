(ns hunter-etl.portals.data-gov
  (:require [hunter-etl.transform :refer :all]
            [hunter-etl.util :refer :all]
            [hunter-etl.extract :refer :all]))

;;;; extract

(def dg-url
  "data.gov API url: https://catalog.data.gov/api"
  "https://catalog.data.gov/api")

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

(defn get-temporal-us
  "if possible, returns the extended vector of the cleaned temporal"
  [extras]
  (if-not (nil? (clean-temporal extras))
    (extend-temporal (clean-temporal extras))
    "all"))

(defn dg-huntscore
  "calculate huntscore for data.gov"
  [recent views]
  (calculate-huntscore 0 recent views 0))

(deftransform dg-transform
  [:title :notes :organization :resources :tags :extras
   :revision_timestamp :tracking_summary]

  {}

  {:title       [identity :title]
   :description [notes->description :notes :title]
   :publisher   [identity [:organization :title]]
   :uri         [url->uri [:resources 0 :url]]
   :created     [get-created [:resources 0 :created] :revision_timestamp]
   :updated     [identity :revision_timestamp]
   :tags        [tags-with-title :title :tags]
   :spatial     [(geo-tagify "us")]
   :temporal    [get-temporal-us :extras]
   :resources   [clean-resources :resources :title]
   :huntscore   [dg-huntscore [:tracking_summary :recent] [:tracking_summary :total]]})
