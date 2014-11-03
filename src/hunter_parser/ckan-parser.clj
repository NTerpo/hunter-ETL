(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]))

(def base-url "https://catalog.data.gov/api/3/")

(defn get-result
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

(defn get-10-most-pop-datagov-ds
  []
  (let [response (((get-result "https://catalog.data.gov/api/3/action/package_search?q=&rows=10") :result) :results)]
    ;; response
    (map #(select-keys % [:title]) response)))






(comment
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
     "results")))
