(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(def base-url "https://catalog.data.gov/api/3/")

(defn get-result
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

(defn- get-tags
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn- get-temporal
  [vect]
  (first
   (filter #(not (nil? %))
           (map #(get % :temporal)
                (map #(hash-map (keyword (% :key)) (% :value))
                     (map #(select-keys % [:key :value]) vect))))))

(defn get-10-most-pop-datagov-ds
  []
  (let [response (((get-result "https://catalog.data.gov/api/3/action/package_search?q=&rows=10") :result) :results)]
    (->> (map #(select-keys % [:title :notes :organization :resources :tags :extras :revision_timestamp]) response)
         (map #(assoc % :publisher (get-in % [:organization :title])
                      :uri (get-in % [:resources 0 :url])
                      :created (get-in % [:resources 0 :created])
                      :tags (get-tags (% :tags))
                      :spatial "USA"
                      :temporal (get-temporal (% :extras))
                      :updated (% :revision_timestamp)
                      :description (% :notes)))
         (map #(dissoc % :organization :resources :extras :revision_timestamp :notes)))))
