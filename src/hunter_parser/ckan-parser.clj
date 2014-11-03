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

(defn- get-tags
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn get-10-most-pop-datagov-ds
  []
  (let [response (((get-result "https://catalog.data.gov/api/3/action/package_search?q=&rows=10") :result) :results)
        filtered (->> (map #(select-keys % [:title :notes :organization :resources :tags :extras :revision_timestamp]) response)
                      (map #(assoc % :publisher (get-in % [:organization :title])))
                      (map #(assoc % :uri (get-in % [:resources 0 :url])))
                      (map #(assoc % :created (get-in % [:resources 0 :created])))
                      (map #(assoc % :tags (get-tags (% :tags))))
                      (map #(assoc % :spatial "USA"))
                      (map #(assoc % :temporal (get-in % [:extras]))))]
    (->> filtered
         (map #(dissoc % :organization))
         (map #(dissoc % :resources))
         (map #(dissoc % :extras)))))






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
