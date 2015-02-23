(ns hunter-etl.extract
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(defn get-result
  "gets metadata from an API and provides a first basic filter"
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

(defn extract-from-ckan-v3
  "extract data from most of the CKAN API v3
  takes as argument the api URL and optionnaly
  the number of dataset's metadata wanted,
  an offset and a query string"
  ([base-api] (extract-from-ckan-v3 base-api 1 0 ""))
  
  ([base-api number] (extract-from-ckan-v3 base-api number 0 ""))

  ([base-api number offset] (extract-from-ckan-v3 base-api number offset ""))
  
  ([base-api number offset request]
   (((get-result (str base-api "/3/"
                      "action/package_search?"
                      "q=" request
                      "&rows=" number
                      "&start=" offset)) :result) :results)))

(defn extract-from-ckan-v3-with-sort
  "extract data from most of the CKAN API v3
  takes as argument the api URL and optionnaly
  the number of dataset's metadata wanted,
  an offset and a query string"
  ([base-api sort] (extract-from-ckan-v3-with-sort base-api sort 1 0 ""))
  
  ([base-api sort number] (extract-from-ckan-v3-with-sort base-api sort number 0 ""))

  ([base-api sort number offset] (extract-from-ckan-v3-with-sort base-api sort number offset ""))
  
  ([base-api sort number offset request]
   (((get-result (str base-api "/3/"
                      "action/package_search?"
                      "q=" request
                      sort
                      "&rows=" number
                      "&start=" offset)) :result) :results)))

(defn extract-from-ckan-v1
  "extract data from most of the CKAN API v1
  takes as argument the api URL and optionnaly
  the number of dataset's metadata wanted,
  an offset - multiple of number as the offset is in
  fact the page number - and a query string"
  ([base-api] (extract-from-ckan-v1 base-api 1 1 ""))
  
  ([base-api number] (extract-from-ckan-v1 base-api number 1 ""))

  ([base-api number offset] (extract-from-ckan-v1 base-api number offset ""))
  
  ([base-api number offset request]
   ((get-result (str base-api "/1/"
                     "datasets/?"
                     "q=" request
                     "&sort=-reuses"
                     "&page_size=" number
                     "&page=" offset)) :data)))
