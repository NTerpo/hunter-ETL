(ns hunter-etl.ckan
  (:require [hunter-etl.transform :refer :all]))

;;;; Basic transformation function used with CKAN API ETL

;;; extract

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

;;; transform

(defn get-tags
  "With the CKAN API, tags are in a vector of maps.
  Only the name of each tag is needed"
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn tags-with-title
  "concat tags and tagified title"
  [title tags]
  (vec (concat (tagify-title title)
               (extend-tags tags))))

(defn notes->description
  "if notes are present, returns them
  else, returns the dataset title"
  [notes title]
  (if (and (not-empty notes)
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

