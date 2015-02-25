(ns hunter-etl.portals.energy-data-usa
  (:require [hunter-etl.transform :refer :all]
            [hunter-etl.util :refer :all]
            [hunter-etl.extract :refer :all]))

;;;; extract

(def edex-url
  "Energy Data EXchange USA API url: https://edx.netl.doe.gov/api"
  "https://edx.netl.doe.gov/api")

(defn edex-extract
  "extract data from the Energy Data EXchange USA API and clean
  the introduction returns a collection of datasets metadata"
  [& args]
  (apply extract-from-ckan-v3-with-sort edex-url "&sort=views_recent+desc" args))

;;;; transform

(defn edex-publisher
  ""
  [s]
  (if (not-empty s)
    s
    "NETL's Energy Data eXchange"))

(defn edex-huntscore
  "calculate huntscore for NETL's Energy Data eXchange data"
  [recent views]
  (calculate-huntscore 5 recent views 0))

(deftransform edex-transform
  [:title :notes :organization :url :metadata_created :metadata_modified :revision_timestamp :tags :locations :resources :tracking_summary]
  {}
  {:title       [identity :title]
   :description [notes->description :notes :title]
   :publisher   [edex-publisher [:organization :title]]
   :uri         [url->uri :url]
   :created     [get-created :metadata_created :revision_timestamp]
   :updated     [get-created :metadata_modified :revision_timestamp]
   :tags        [tags-with-title :title :tags]
   :spatial     [identity :locations]
   :temporal    [["all"]]
   :resources   [clean-resources :resources :title]
   :huntscore   [edex-huntscore [:tracking_summary :recent] [:tracking_summary :total]]})
