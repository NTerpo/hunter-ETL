(ns hunter-parser.opendata-europa
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://open-data.europa.eu/data/api/action/package_search?q=")

;; curl -i http://open-data.europa.eu/data/api/action/package_search -d '{"q": ""}'

(defn clean-response
  [response]
  (let [resp (map :metatype (:datacatalog response))
        clean (fn [ds] (apply array-map (mapcat #(vector (keyword (:id %)) (:value %)) ds)))]
    (vec (map clean resp))))



;;;;
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


