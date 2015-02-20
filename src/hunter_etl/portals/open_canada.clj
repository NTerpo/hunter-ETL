(ns hunter-etl.portals.open-canada
  (:require [hunter-etl.transform :refer :all]
            [hunter-etl.util :refer :all]
            [hunter-etl.extract :refer :all]))

;;;; extract

(def ca-url
  "open.canada.ca API url: http://open.canada.ca/data/en/api"
  "http://open.canada.ca/data/en/api")

(defn ca-extract
  "extract data from the open.canada.ca API and clean the instruction
  returns a collection of datasets metadata"
  [number offset]
  (extract-from-ckan-v3 ca-url number offset "&sort=metadata_modified+desc"))

;;;; transform

(defn str+
  "concatenates two string and add a space between them"
  [str1 str2]
  (str str1 " " str2))

(defn notes->description-ca
  "concatenate the notes in english and french and if notes are
  present, returns them else, returns the dataset title"
  [notes_en notes_fra title]
  (let [notes (str+ notes_en notes_fra)]
    (if (and (not-empty notes)
             (not= " " notes))
      notes
      title)))

(defn get-created
  "try to get the created dataset date"
  [resource alt]
  (if-not (nil? resource)
    resource
    alt))

(deftransform ca-transform
  [:title :title_fra :notes :notes_fra :organization :url :metadata_created :metadata_modified :tags :time_period_coverage_start :time_period_coverage_end :resources :tracking_summary :revision_timestamp]
  {}
  {:title       [str+ :title :title_fra]
   :description [notes->description-ca :notes :notes_fra :title]
   :publisher   [identity [:organization :title]]
   :uri         [url->uri :url]
   :created     [get-created :metadata_created :revision_timestamp]
   :updated     [get-created :metadata_modified :revision_timestamp]
   :tags        [tags-with-title :title :tags]
   :spatial     [(geo-tagify "canada")]
   :temporal    [get-temporal :time_period_coverage_start :time_period_coverage_end]
   :resources   [clean-resources :resources :title]
   :huntscore   [(calculate-huntscore 5 0 0 0)]})
