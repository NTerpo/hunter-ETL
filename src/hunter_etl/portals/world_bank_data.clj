(ns hunter-etl.portals.world-bank-data
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.transform :refer :all]
            [hunter-etl.ckan :refer :all]))

;;;; extract

(def wb-url
  "worldbank API url: http://api.worldbank.org"
  "http://api.worldbank.org")

(defn clean-response
  "clean the response from World Bank API"
  [response]
  (let [resp (map :metatype (:datacatalog response))
        clean (fn [ds] (apply array-map (mapcat #(vector (keyword (:id %)) (:value %)) ds)))]
    (vec (map clean resp))))

(defn wb-extract
  "extract data from the World Bank API and clean the introduction
  returns a collection of datasets metadata
  number : number of dataset per page
  offset : page number "
  ([] (wb-extract 1 1))
  ([number] (wb-extract number 1))
  ([number offset]
   (clean-response (get-result
                    (str wb-url
                         "/v2/datacatalog?format=json"
                         "&per_page=" number
                         "&page=" offset)))))

;;;; transform

(defn parse-and-convert-date
  [string]
  (let [date-map (if (or (= "Current" string)
                         (nil? string))
                   ["30" "nov" "2014"]
                   (if (= "April, 2010" string)
                     ["01" "apr" "2010"]
                     (-> string st/lower-case (st/split #"-"))))
        convert-m {"jan" "01"
                   "feb" "02"
                   "mar" "03"
                   "apr" "04"
                   "may" "05"
                   "jun" "06"
                   "jul" "07"
                   "aug" "08"
                   "sep" "09"
                   "oct" "10"
                   "nov" "11"
                   "dec" "12"}
        date (zipmap [:day :month :year] date-map)
        clean-date (conj date {:month (convert-m (:month date))})]
    (str (clean-date :year) "-"
         (clean-date :month) "-"
         (clean-date :day))))

(defn get-publisher
  "get the publisher and returns WorldBank Data if it isn't available"
  [publisher]
  (if-not (nil? publisher)
    publisher
    "World Bank Data"))

(defn get-wb-tags
 "get the tags from the topic key"
 [tags]
 (st/split tags #" "))

(defn tags-with-title-wb
 "concat tags and tagified title"
 [title tags]
 (vec (concat (tagify-title title)
              (extend-tags (get-wb-tags tags)))))

(defn get-temporal-wb
  "get the temporal coverage from world bank datasets"
  [coverage]
  (if (not-empty coverage)
    (extend-temporal coverage)
    "all"))

(deftransform wb-transform
  [:name :description :url :granularity :topics :lastrevisiondate :cite :coverage]
  {}
  {:title       [identity :name]
   :description [notes->description :description :name]
   :publisher   [get-publisher :cite]
   :uri         [url->uri :url]
   :created     [parse-and-convert-date :lastrevisiondate]
   :updated     [parse-and-convert-date :lastrevisiondate]
   :spatial     [(geo-tagify "world")]
   :temporal    [get-temporal-wb :coverage]
   :tags        [tags-with-title-wb :name :topics]
   :resources   [[:foo "bar"]]
   :huntscore   [(calculate-huntscore 5 0 0 0)]})

