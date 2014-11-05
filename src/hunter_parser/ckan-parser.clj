(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(def base-url "https://catalog.data.gov/api/3/")
(def api-url "http://localhost:3000/api/datasets")

(defn get-result
  "gets a dataset metadata and provides a first basic filter"
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

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

(defn get-most-pop-datagov-ds
  "gets a number of the most popular datasets' metadata from the ckan API of data.gov and transforms them to match the Hunter API scheme"
  [number]
  (let [response (((get-result (str "https://catalog.data.gov/api/3/action/package_search?q=&rows=") number) :result) :results)]
    (->> (map #(select-keys % [:title :notes :organization :resources :tags :extras :revision_timestamp]) response)
         (map #(assoc % :publisher (get-in % [:organization :title])
                      :uri (get-in % [:resources 0 :url])
                      :created (get-in % [:resources 0 :created])
                      :tags (get-tags (% :tags))
                      :spatial "USA"
                      :temporal (if (not (nil? (get-temporal (% :extras))))
                                  (get-temporal (% :extras))
                                  "all")
                      :updated (% :revision_timestamp)
                      :description (% :notes)))
         (map #(dissoc % :organization :resources :extras :revision_timestamp :notes)))))

(defn export-datasets-to-hunter-api
  "Send each dataset to the Hunter API via method post: (export-datasets-to-hunter-api (get-most-po-datagov-ds 10))"
  [coll]
  (map #(client/post api-url
                     {:body (generate-string %)
                      :content-type "application/json"}) coll))
