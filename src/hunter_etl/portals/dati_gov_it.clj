(ns hunter-etl.portals.dati-gov-it
  (:require [hunter-etl.transform :refer :all]
            [hunter-etl.util :refer :all]
            [hunter-etl.extract :refer :all]))

;;;; extract

(def dgi-url
  "dati.gov.it API url: http://www.dati.gov.it/catalog/api"
  "http://www.dati.gov.it/catalog/api")

(defn dgi-extract
  "extract data from the dati.gov.it API and clean the introduction
  returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan-v3-with-sort dgi-url "&sort=views_recent+desc" args))

;;;; transform

(defn dgi-publisher
  "get publisher or returns dati.gov.it"
  [s]
  (if (not-empty s)
    s
    "dati.gov.it"))

(defn dgi-huntscore
  "calculate huntscore for dati.gov.it"
  [recent total]
  (calculate-huntscore 5 recent total 0))

(deftransform dgi-transform
  [:title :notes :organization :url :metadata_created :metadata_modified :revision_timestamp :tags :resources :tracking_summary]
  {}
  {:title [identity :title]
   :description [notes->description :notes :title]
   :publisher   [dgi-publisher [:organization :title]]
   :uri         [url->uri :url]
   :created     [get-created :metadata_created :revision_timestamp]
   :updated     [get-created :metadata_modified :revision_timestamp]
   :tags        [tags-with-title :title :tags]
   :spatial     [["italia", "italy", "europe", "world"]]
   :temporal    [["all"]]
   :resources   [clean-resources :resources :title]
   :huntscore   [dgi-huntscore [:tracking_summary :recent] [:tracking_summary :total]]})
