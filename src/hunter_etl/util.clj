(ns hunter-etl.util
  (:require [clojure.string :as st]))

;;;; Classics

;; Description

(defn notes->description
  "if notes are present, returns them
  else, returns the dataset title"
  [notes title]
  (if (and (not-empty notes)
           (not= " " notes))
    notes
    title))

;; URI

(defn url->uri
  "returns an error string if the url is missing"
  [url]
  (if (not-empty url)
    url
    "URI Not Available"))

;; Dates

(defn get-created
  "try to get the created dataset date"
  [resource alt]
  (if-not (nil? resource)
    resource
    alt))

;; Spatial 

(defn geo-tagify
  "extend the spatial coverage tagging 'us'->'america'->'countries'->'world'"
  [geo]
  (let [geo (st/lower-case geo)]
    (if-not (nil? (some #{geo} ["france" "us" "europe" "world" "uk" "canada"]))
      ({"france" ["france" "fr" "europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "us" ["us" "usa" "america" "united states" "united-states" "united states of america" "united-states-of-america" "world" "countries" "all"]
        "europe" ["europe" "schengen" "eu" "ue" "countries" "world" "all"]
        "world" ["world" "all" "countries"]
        "uk" ["uk" "england" "scotland" "wales" "ireland" "great-britain" "gb"]
        "canada" ["canada" "world" "all"]} geo)
      (vector geo))))

;; Temporal

(defn extend-temporal
  "extend the temporal coverage with dates between limits"
  [temporal]
  (let [limits (re-seq #"[0-9]{4}" temporal)]
    (if (nil? limits)
      "all"
      (if (= 1 (count limits))
                        (vec limits)
                        (vec
                         (map str
                              (range (Integer. (first limits))
                                     (+ 1 (Integer. (last limits))))))))))

(defn get-temporal
  "returns, if available the temporal coverage
  If it's not possible it try to find the temporal
  coverage from the resources"
  [from to]
  (if (and (not-empty from)
           (not-empty to))
    (extend-temporal (str from "/" to))
    "all"))

;; Tags

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

(defn get-tags
  "With the CKAN API, tags are in a vector of maps.
  Only the name of each tag is needed"
  [vect]
  (vec (map #(% :name) (map #(select-keys % [:name]) vect))))

(defn tags-with-title
  "concat tags and tagified title"
  [title tags]
  (vec (concat (tagify-title title)
               (extend-tags (get-tags tags)))))

;; Resources

(defn clean-resources
  "returns a vector of resources limited to format, url and title"
  [coll title]
  (vec (->> (map #(select-keys % [:format :url]) coll)
            (map #(assoc % :title title)))))

;; Huntscore

(defn calculate-huntscore
  "returns the sum of reuses, views/1000 recent-views/200 and followers/10"
  ([reuses] (calculate-huntscore reuses 0 0 0))
  ([reuses recent] (calculate-huntscore reuses recent 0 0))
  ([reuses recent views] (calculate-huntscore reuses recent views 0))
  ([reuses recent views followers]
   (reduce + [(* 1 reuses)
              (* 0.005 recent)
              (* 0.001 views)
              (* 0.1 followers)])))
