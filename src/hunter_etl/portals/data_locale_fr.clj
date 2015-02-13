(ns hunter-etl.portals.data-locale-fr
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-etl.util :refer :all]))

(defn get-datalocale-ds
  "gets all datasets' metadata from the ckan API of datalocale.fr and transforms them to match the Hunter API scheme"
  []
  (let [response (get-result "http://catalogue.datalocale.fr//storage/f/2014-03-19T094540/datalocale-20140320-daily.json")]
    (->> (map #(select-keys % [:title :notes :author :resources :tags :extras]) response)
         (map #(assoc % :publisher (% :author)
                      :uri (if-not (nil? (get-in % [:resources 0 :url]))
                             (get-in % [:resources 0 :url])
                             "URI Not Available")
                      :created (get-in % [:resources 0 :created])
                      :updated (if-not (nil? (get-in % [:resources 0 :last_modified]))
                                 (get-in % [:resources 0 :last_modified])
                                 (get-in % [:resources 0 :created]))
                      :tags (% :tags)
                      :spatial (if-not (nil? (get-in % [:extras "spatial-text"]))
                                 (get-in % [:extras "spatial-text"])
                                 "Aquitaine")
                      :temporal (str (get-in % [:extras "temporal_coverage-from"]) "-" (get-in % [:extras "temporal_coverage-to"]))
                      :description (% :notes)))
         (map #(dissoc % :resources :extras :notes)))))
