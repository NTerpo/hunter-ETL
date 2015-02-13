(ns hunter-etl.ckan
  (:require [hunter-etl.util :refer :all]))

;;;; Basic transformation function used with CKAN API ETL

;;; extract

(defn extract-from-ckan
  "extract data from most of the CKAN API
  takes as argument the api URL and optionnaly
  the number of dataset's metadata wanted,
  an offset and a query string"
  ([base-api] (extract-from-ckan base-api 1 0 ""))
  
  ([base-api number] (extract-from-ckan base-api number 0 ""))

  ([base-api number offset] (extract-from-ckan base-api number offset ""))
  
  ([base-api number offset request]
   (((get-result (str base-api "/3/"
                      "action/package_search?"
                      "q=" request
                      "&rows=" number
                      "&start=" offset))
     :result)
    :results)))

;;; transform

(defn get-tags
  "With the CKAN API, tags are in a vector of maps.
  Only the name of each tag is needed"
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn notes->description
  "if notes are present, returns them
  else, returns the dataset title"
  [notes title]
  (if (or (not-empty notes)
          (not= " " notes))
    notes
    title))

(defn url->uri
  "returns an error string if the url is missing"
  [url]
  (if (not-empty url)
    url
    "URI Not Available"))

(defn clean-resources
  "returns a vector of resources limited to format, url and title"
  [coll title]
  (vec (->> (map #(select-keys % [:format :url]) coll)
            (map #(assoc % :title title)))))
