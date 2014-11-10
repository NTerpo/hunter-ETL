(ns hunter-parser.datagouvfr
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(def base-url "http://www.data.gouv.fr/api/1/")
;; (def api-key "eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyIjoiNTM3MDBiYzFhM2E3Mjk0NjAwNDM1ZmJkIiwidGltZSI6MTQxNTYyODcyMi40NzQ3NzZ9.MLofKql5iK7JR1LdfQZfUxjYjA194i5gfIbIe0IZM1Q")

(defn get-datagouvfr-ds
  "gets a number of the most popular datasets' metadata from the API of data.gouv.fr and transforms them to match the Hunter API scheme"
  [number]
  (let [response ((parse-string (:body (client/get (str base-url "datasets/?sort=-reuses"
                                                         "&page_size=" number))) true) :data)]
    (->> (map #(select-keys % [:title :uri :description :last_modified ]) response)
         )))
