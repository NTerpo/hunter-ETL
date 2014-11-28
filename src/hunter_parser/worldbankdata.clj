(ns hunter-parser.worldbankdata
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://api.worldbank.org/v2/datacatalog?format=json&per_page=99")

(defn clean-response
  [response]
  (let [resp (map :metatype (:datacatalog response))
        clean (fn [ds] (apply array-map (mapcat #(vector (keyword (:id %)) (:value %)) ds)))]
    (vec (map clean resp))))

(defn get-worldbank-ds
  "gets a number of the most popular datasets' metadata from the World Bank Data API and transforms them to match the Hunter API scheme"
  []
  (let [response (clean-response (get-result base-url))]
    (->> (map #(select-keys % [:name :description :url :granularity :topics :lastrevisiondate :cite :coverage]) response)
         (map #(assoc %
                 :publisher (if-not (nil? (% :cite))
                              (% :cite)
                              "World Bank Data")
                 :uri (if-not (nil? (% :url))
                        (% :url)
                        "URI Not Available")
                 :created (when-not (nil? (% :lastrevisiondate))
                            (% :lastrevisiondate))
                 :tags (vec (concat (tagify-title (% :name))
                                    (extend-tags (st/split (% :topics) #" "))))
                 :spatial (geo-tagify "world")
                 :temporal (if (not (nil? (% :coverage)))
                             (extend-temporal (% :coverage))
                             "all")
                 :updated (% :lastrevisiondate)
                 :description (if-not (nil? (% :description))
                                (% :description)
                                (% :name))
                 :huntscore (calculate-huntscore 5 0 0 0)))
         (map #(dissoc % :name :granularity :topics :url :lastrevisiondate :cite :coverage)))))

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
                 :huntscore (calculate-huntscore 0
                                                 (get-in % [:tracking_summary :total])
                                                 (get-in % [:tracking_summary :recent])
                                                 0)))
         (map #(dissoc % :organization :resources :extras :revision_timestamp :notes :tracking_summary)))))
