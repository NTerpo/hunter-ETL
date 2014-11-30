(ns hunter-parser.opendata-europa
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://open-data.europa.eu/data/api/action/package_search")

(defn get-response
  [url]
  (let [resp (:body (client/post base-url
                                 {:body "{\"q\": \"\"}"
                                  :content-type :json
                                  :accept :json}))
        clean-resp (:results (:result (parse-string resp true)))]
    clean-resp))

(defn- get-tags
  [vect]
  (vec (map #(% :name) vect)))

(defn get-europa-opendata-ds
  "gets a number of the most popular datasets' metadata from the Europa Open Data API  and transforms them to match the Hunter API scheme"
  []
  (let [response (get-response base-url)]
    (->> (map #(select-keys % [:description :temporal_coverage_from :metadata_created :temporal_coverage_to :keywords :title :contact_name :geographical_coverage :url :modified_date :resources]) response)
         (map #(assoc %
                 :uri (% :url)
                 :publisher (if-not (nil? (% :contact_name))
                              (read-string (% :contact_name))
                              "open-data.europa.eu")
                 :spatial (if-not (empty? (% :geographical_coverage))
                            (geo-tagify (% :geographical_coverage))
                            (geo-tagify "europe"))
                 :tags (vec (concat (tagify-title (% :title))
                                    (extend-tags (get-tags (% :keywords)))))
                 :description (if-not (empty? (% :description))
                                (% :description)
                                (% :title))
                 :temporal (if-not (empty? (% :temporal_coverage_from))
                             (extend-temporal (str (% :temporal_coverage_from)
                                                   "/"
                                                   (if-not (empty? (% :temporal_coverage_to))
                                                     (% :temporal_coverage_to)
                                                     "2014")))
                             "all")
                 :updated (if-not (empty? (% :modified_date))
                            (read-string (% :modified_date))
                            (% :metadata_created))
                 :created (if-not (nil? (get-in % [:resources 0 :created]))
                            (get-in % [:resources 0 :created])
                            (% :modified_date))
                 :huntscore (calculate-huntscore 5 0 0 0)))
         (map #(dissoc % :temporal_coverage_from :temporal_coverage_to :keywords :contact_name :url :geographical_coverage :modified_date :resources :metadata_created)))))

