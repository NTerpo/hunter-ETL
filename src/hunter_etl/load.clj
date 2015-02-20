(ns hunter-etl.load
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(def api-url
  "URL of the Hunter API
  used to POST the datasets after the transformations"
  "http://localhost:3000/api/datasets")

(defn load-to-hunter-api
  "Send each dataset to the Hunter API via method post"
  [coll]
  (map #(client/post api-url
                     {:body (generate-string %)
                      :content-type "application/json"}) coll))
