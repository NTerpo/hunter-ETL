(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]))

(def base-url "https://catalog.data.gov/api/3/")
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
    (if-not (nil? (some #{geo} ["france" "us" "europe"]))
      ({"france" ["france" "fr" "europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "us" ["us" "usa" "america" "united states" "united-states" "united states of america" "united-states-of-america" "world" "countries" "all"]
        "europe" ["europe" "schengen" "eu" "ue" "countries" "world" "all"]} geo)
      geo)))

(defn extend-tags
  "create new tags with the given tags vector by spliting words and cleaning"
  [tags]
  (vec (disj (set (->> (map st/lower-case tags)
                       (map st/trim)
                       (mapcat #(st/split % #"-"))
                       (concat tags))) "report" "data" "service" "government")))

(defn tagify-title
  "create new tags from the title"
  [title]
  (vec (disj (set (-> (st/lower-case title)
                      (st/split #" "))) "database" "db" "data" "dataset")))

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

;;
;; data.gov
;;

(defn- get-tags
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn- get-temporal
  "try to find the value of 'temporal' from a data.gov metadata dataset"
  [vect]
  (first
   (filter #(not (nil? %))
           (map #(get % :temporal)
                (map #(hash-map (keyword (% :key)) (% :value))
                     (map #(select-keys % [:key :value]) vect))))))

(defn get-most-pop-datagov-ds
  "gets a number of the most popular datasets' metadata from the ckan API of data.gov and transforms them to match the Hunter API scheme"
  [number]
  (let [response (((get-result (str "https://catalog.data.gov/api/3/action/package_search?q=&rows=" number)) :result) :results)]
    (->> (map #(select-keys % [:title :notes :organization :resources :tags :extras :revision_timestamp]) response)
         (map #(assoc % :publisher (get-in % [:organization :title])
                      :uri (get-in % [:resources 0 :url])
                      :created (get-in % [:resources 0 :created])
                      :tags (vec (concat (tagify-title (% :title)) (extend-tags (get-tags (% :tags)))))
                      :spatial (geo-tagify "us")
                      :temporal (if (not (nil? (get-temporal (% :extras))))
                                   (get-temporal (% :extras))
                                  "all")
                      :updated (% :revision_timestamp)
                      :description (% :notes)))
         (map #(dissoc % :organization :resources :extras :revision_timestamp :notes)))))

;;
;; datalocale - Portail Open Data de l'Aquitaine
;;

(defn get-datalocale-ds
  "gets all datasets' metadata from the ckan API of datalocale.fr and transforms them to match the Hunter API scheme"
  []
  (let [response (get-result "http://catalogue.datalocale.fr//storage/f/2014-03-19T094540/datalocale-20140320-daily.json")]
    (->> (map #(select-keys % [:title :notes :author :resources :tags :extras]) response)
         (map #(assoc % :publisher (% :author)
                      :uri (get-in % [:resources 0 :url])
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

;;
;; Export
;;

(defn export-datasets-to-hunter-api
  "Send each dataset to the Hunter API via method post"
  [coll]
  (map #(client/post api-url
                     {:body (generate-string %)
                      :content-type "application/json"}) coll))
