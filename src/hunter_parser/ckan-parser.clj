(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(def base-url "https://catalog.data.gov/api/3/")

(defn get-package-list
  []
  (-> (str base-url "action/package_list?limit=1")
      (client/get)
      :body
      (parse-string)))

(defn get-package-with-query
  [query]
  (((-> (str base-url "action/package_search?q=")
         (str query)
         (client/get)
         :body
         (parse-string))
     "result")
   "results"))
