(ns hunter-etl.core
  (:use [clojure.data :refer :all])
  (:require [hunter-etl.util :refer :all]))

(defn extract-from-ckan
  ([base-api] (extract-from-ckan base-api 1 0 ""))
  
  ([base-api number] (extract-from-ckan base-api number 0 ""))

  ([base-api number offset] (extract-from-ckan base-api number offset ""))
  
  ([base-api number offset request]
   (((get-result (str base-api "/3/" "action/package_search?"
                      "q=" request
                      "&rows=" number
                      "&start=" offset))
     :result)
    :results)))

(def hunter-keys
  "keys needed in the Hunter API"
  [:title :description :publisher :uri :created :updated :spatial :temporal :tags :resources :huntscore])

(defn not-hunter-keys
  "keys on a collection that are not on the hunter-keys"
  [vect]
  (seq
   (second
    (diff (set hunter-keys) (set vect)))))

(defn uk-extract
  [& [number offset request]]
  (extract-from-ckan "http://data.gov.uk/api" number offset request))

(defn uk-transform
  [coll]
  (let [ks [:title :notes :organization :resources :tags :tracking_summary :temporal_coverage-to :metadata_created :metadata_modified :temporal_coverage-from :geographic_coverage :url]
        nks (not-hunter-keys ks)]
    (->> coll
         ;; (filter published?)
         ;; (filter resources?)
         (map #(select-keys % ks))
         ;; (map #(assoc %
         ;;         :description (notes->description (% :notes))
         ;;         :uri (url->uri (% :url))
         ;;         :publisher (org->publisher (% :organization))))
         (map #(dissoc % nks)))))

(defn uk-load
  [coll]
  (export-datasets-to-hunter-api coll))

(defn uk-etl
  [& [number offset request]]
  (-> (uk-extract number offset request)
      uk-transform
      #_uk-load))

(defn -main
  ""
  []
  (println "Hunter ETL"))
