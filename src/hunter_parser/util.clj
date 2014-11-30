(ns hunter-parser.util
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]))

(def api-url "http://localhost:3000/api/datasets")

(defn get-result
  "gets a dataset metadata and provides a first basic filter"
  [url]
  (-> url
      (client/get)
      :body
      (parse-string true)))

(defn geo-tagify
  "extend the spatial coverage tagging 'us'->'america'->'countries'->'world'"
  [geo]
  (let [geo (clojure.string/lower-case geo)]
    (if-not (nil? (some #{geo} ["france" "us" "europe" "world"]))
      ({"france" ["france" "fr" "europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "us" ["us" "usa" "america" "united states" "united-states" "united states of america" "united-states-of-america" "world" "countries" "all"]
        "europe" ["europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "world" ["world" "all" "countries"]} geo)
      (vector geo))))

(defn extend-tags
  "create new tags with the given tags vector by spliting words and cleaning"
  [tags]
  (->> (vec (disj (set (->> (map st/lower-case tags)
                            (map st/trim)
                            (mapcat #(st/split % #"-"))
                            (concat tags))) "report" "data" "-" "service" "government"))
       (map #(st/replace % "," ""))
       (map #(st/replace % "(" ""))
       (map #(st/replace % ")" ""))))

(defn tagify-title
  "create new tags from the title"
  [title]
  (vec (disj (set (-> (st/lower-case title)
                      (st/split #" "))) "database" "-" "db" "data" "dataset" "to" "and")))

(defn extend-temporal
  "extend the temporal coverage with dates between limits"
  [temporal]
  (let [limits (re-seq #"[0-9]{4}" temporal)]
    (if (= 1 (count limits))
      (vec limits)
      (vec
       (map str
            (range (Integer. (first limits))
                   (+ 1 (Integer. (last limits)))))))))

(defn calculate-huntscore
  ""
  [reuses total-views recent-views followers]
  (reduce + [(* 1 reuses)
             (* 0.001 total-views)
             (* 0.005 recent-views)
             (* 0.1 followers)]))

;;
;; Export
;;

(defn export-datasets-to-hunter-api
  "Send each dataset to the Hunter API via method post"
  [coll]
  (map #(client/post api-url
                     {:body (generate-string %)
                      :content-type "application/json"}) coll))
