(ns hunter-parser.datagovuk
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://data.gov.uk/api/3/")

(defn get-tags
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn clean-resources
  [coll title]
  (vec (->> (map #(select-keys % [:format :url]) coll)
            (map #(assoc % :title title)))))

(defn get-resource-temporal
  [vect]
  (let [f (first vect)
        l (last vect)
        from (re-seq #"[0-9]{4}" (get-in f [:description]))
        to (re-seq #"[0-9]{4}" (get-in l [:description]))]
    (if-not (and (empty? from)
                 (empty? to))
      (extend-temporal (str from "/" to))
      "all")))

(defn get-datagov-uk-ds
  "gets a number of the most popular datasets' metadata from the ckan API of data.gov.uk and transforms them to match the Hunter API scheme"
  [number offset]
  (let [response (((get-result (str base-url "action/package_search?q="
                                    "&rows=" number
                                    "&start=" offset))
                   :result)
                  :results)]
    (->> (map #(select-keys % [:title :notes :organization :resources :tags :tracking_summary :temporal_coverage-to
                               :metadata_created :metadata_modified :temporal_coverage-from :geographic_coverage :url]) response)
         (map #(assoc %
                 :description (if-not (nil? (% :notes))
                                (% :notes)
                                (% :title))
                 :uri (if-not (nil? (% :url))
                        (% :url)
                        "URI Not Available")
                 :publisher (get-in % [:organization :title])
                 :created (% :metadata_created) ; TODO: check nil
                 :updated (% :metadata_modified) ; TODO: check nil
                 :spatial (geo-tagify "uk") ; TODO: expend UK
                 :temporal (if-not (and (empty? (% :temporal_coverage-from))
                                        (empty? (% :temporal_coverage-to)))
                             (extend-temporal (str (% :temporal_coverage-from) "/"                                                                            (% :temporal_coverage-to)))
                             (get-resource-temporal (% :resources)))
                 :tags (vec (concat (tagify-title (% :title)) ; TODO: check nil
                                    (extend-tags (get-tags (% :tags)))))
                 :resources (clean-resources (% :resources) (% :title))
                 :huntscore (calculate-huntscore 0 ; TODO: find a way
                                        ; to give a score
                                                 (get-in % [:tracking_summary :total])
                                                 (get-in % [:tracking_summary :recent])
                                                 0)))
         (map #(dissoc % :notes :temporal_coverage-to :temporal_coverage-from :tracking_summary
                       :metadata_created :metadata_modified :geographic_coverage :url :organization)))))

