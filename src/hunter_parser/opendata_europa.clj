(ns hunter-parser.opendata-europa
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://open-data.europa.eu/data/api/action/package_search")

(defn get-response
  [url]
  (let [resp (:body (client/post base-url
                                 {:body "{\"q\": \"\", \"rows\":\"100\", \"sort\":\"views_total desc\"}"
                                  :content-type :json
                                  :accept :json}))
        clean-resp (:results (:result (parse-string resp true)))]
    clean-resp))

(defn- get-tags
  [vect]
  (vec (map #(% :name) vect)))

(defn clean-resource-url [url]
  (let [url-v (st/split url #"BulkDownloadListing")
        bulk "BulkDownloadListing?"
        pre (first url-v)
        new-pre "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/"
        suff (last url-v)]
    (if (= "http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/"
             pre)
      (str new-pre bulk suff)
      url)))

(defn clean-resources
  ""
  [coll title]
  (vec (->> (filter #(not (nil? (re-find #"Download dataset in" (% :description)))) coll)
            (map #(select-keys % [:url :description]))
            (map #(assoc % :format (nth (st/split (% :description) #" ") 3)))
            (map #(assoc % :url (clean-resource-url (% :url))))
            (map #(assoc % :title title))
            (map #(dissoc % :description)))))

(defn get-europa-opendata-ds
  "gets a number of the most popular datasets' metadata from the Europa Open Data API  and transforms them to match the Hunter API scheme"
  []
  (let [response (get-response base-url)]
    (->> (map #(select-keys % [:description :temporal_coverage_from :metadata_created :temporal_coverage_to :keywords :title :contact_name :geographical_coverage :url :modified_date :resources]) response)
         (map #(assoc %
                 :uri (% :url)
                 :publisher (if-not (empty? (% :contact_name))
                              (if-not (= "" (read-string (% :contact_name)))
                                (read-string (% :contact_name))
                                "open-data.europa.eu")
                              "open-data.europa.eu")
                 :spatial (geo-tagify "europe")
                 :tags (vec (concat (extend-tags (tagify-title (% :title)))
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
                 :created (if-not (empty? (get-in % [:resources 0 :created]))
                               (get-in % [:resources 0 :created])
                               (if-not (empty? (% :modified_date))
                            (read-string (% :modified_date))
                            (% :metadata_created)))
                 :resources (clean-resources (% :resources) (% :title))
                 :huntscore (calculate-huntscore 5 0 0 0)))
         (map #(dissoc % :temporal_coverage_from :temporal_coverage_to :keywords :contact_name :url :geographical_coverage :modified_date :metadata_created)))))

