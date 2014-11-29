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
;;=> (:body (client/post "http://open-data.europa.eu/data/api/action/package_search"
                                                   ;; {:body "{\"q\": \"\"}"
                                                   ;;  :content-type :json
                                                   ;;  :accept :json}))
;;=> (first (:results (:result (parse-string response true))))


(defn get-worldbank-ds
  "gets a number of the most popular datasets' metadata from the World Bank Data API and transforms them to match the Hunter API scheme"
  []
  (let [response ()]
    (->> (map #(select-keys % [:description :temporal_coverage_from :temporal_coverage_to :keywords :title :contact_name :geographical_coverage :url :modified_date :resources]) response)
         )))

