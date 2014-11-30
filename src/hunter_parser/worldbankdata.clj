(ns hunter-parser.worldbankdata
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://api.worldbank.org/v2/datacatalog?format=json&per_page=120")

(defn clean-response
  [response]
  (let [resp (map :metatype (:datacatalog response))
        clean (fn [ds] (apply array-map (mapcat #(vector (keyword (:id %)) (:value %)) ds)))]
    (vec (map clean resp))))

(defn parse-and-convert-date
  [string]
  (let [date-map (if (or (= "Current" string)
                         (nil? string))
                   ["30" "nov" "2014"]
                   (if (= "April, 2010" string)
                     ["01" "apr" "2010"]
                     (-> string st/lower-case (st/split #"-"))))
        convert-m {"jan" "01"
                   "feb" "02"
                   "mar" "03"
                   "apr" "04"
                   "may" "05"
                   "jun" "06"
                   "jul" "07"
                   "aug" "08"
                   "sep" "09"
                   "oct" "10"
                   "nov" "11"
                   "dec" "12"}
        date (zipmap [:day :month :year] date-map)
        clean-date (conj date {:month (convert-m (:month date))})]
    (str (clean-date :year) "-"
         (clean-date :month) "-"
         (clean-date :day))))

(defn get-worldbank-ds
  "gets a number of the most popular datasets' metadata from the World Bank Data API and transforms them to match the Hunter API scheme"
  []
  (let [response (clean-response (get-result base-url))]
    (->> (map #(select-keys % [:name :description :url :granularity :topics :lastrevisiondate :cite :coverage]) response)
         (map #(assoc %
                 :title (% :name)
                 :publisher (if-not (nil? (% :cite))
                              (% :cite)
                              "World Bank Data")
                 :uri (if-not (nil? (% :url))
                        (% :url)
                        "URI Not Available")
                 :created (parse-and-convert-date (% :lastrevisiondate))
                 :tags (vec (concat (tagify-title (% :name))
                                    (extend-tags (st/split (% :topics) #" "))))
                 :spatial (geo-tagify "world")
                 :temporal (if (not (nil? (% :coverage)))
                             (extend-temporal (% :coverage))
                             "all")
                 :updated (parse-and-convert-date (% :lastrevisiondate))
                 :description (if-not (nil? (% :description))
                                (% :description)
                                (% :name))
                 :huntscore (calculate-huntscore 5 0 0 0)))
         (map #(dissoc % :name :granularity :topics :url :lastrevisiondate :cite :coverage)))))
