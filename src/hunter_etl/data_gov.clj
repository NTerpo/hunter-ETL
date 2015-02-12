(ns hunter-etl.data-gov
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.util :refer :all]))

(def base-url "https://catalog.data.gov/api/3/")

(defn- get-tags
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn- get-temporal
  "try to find the value of 'temporal' from a data.gov metadata dataset"
  [vect]
  (first
   (filter #(not (nil? %))
           (map #(get % :temporal)
                (map #(hash-map (keyword (% :key)) (% :value))
                     (map #(select-keys % [:key :value]) vect))))))

(defn clean-resources
  ""
  [coll title]
  (vec (->> (map #(select-keys % [:format :url]) coll)
            (map #(assoc % :title title)))))

(defn get-datagov-ds
  "gets a number of the most popular datasets' metadata from the ckan API of data.gov and transforms them to match the Hunter API scheme"
  [number offset]
  (let [response (((get-result (str base-url "action/package_search?q="
                                    "&rows=" number
                                    "&start=" offset))
                   :result)
                  :results)]
    (->> (map #(select-keys % [:title :notes :organization :resources :tags :extras :revision_timestamp :tracking_summary]) response)
         (map #(assoc %
                 :publisher (get-in % [:organization :title])
                 :uri (if-not (nil? (get-in % [:resources 0 :url]))
                        (get-in % [:resources 0 :url])
                        "URI Not Available")
                 :created (if-not (nil? (get-in % [:resources 0 :created]))
                            (get-in % [:resources 0 :created])
                            (% :revision_timestamp))
                 :tags (vec (concat (tagify-title (% :title))
                                    (extend-tags (get-tags (% :tags)))))
                 :spatial (geo-tagify "us")
                 :temporal (if (not (nil? (get-temporal (% :extras))))
                             (extend-temporal (get-temporal (% :extras)))
                             "all")
                 :updated (% :revision_timestamp)
                 :description (if-not (nil? (% :notes))
                                (% :notes)
                                (% :title))
                 :resources (clean-resources (% :resources) (% :title))
                 :huntscore (calculate-huntscore 0
                                                 (get-in % [:tracking_summary :total])
                                                 (get-in % [:tracking_summary :recent])
                                                 0)))
         (map #(dissoc % :organization :extras :revision_timestamp :notes :tracking_summary)))))

