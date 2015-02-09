(ns hunter-parser.hand
  (:require [clj-http.client :as client]
            [cheshire.core :refer :all]
            [clojure.string :as st]
            [hunter-parser.util :refer :all]))

(defn create-ds
  "create a hunter dataset by hand
  eg. (create-ds \"Global Bilateral Migration Database\" \"Global matrices of bilateral migrant stocks spanning the period 1960-2000, disaggregated by gender and based primarily on the foreign-born concept are presented. Over one thousand census and population register records are combined to construct decennial matrices corresponding to the last five completed census rounds.\\n\\nFor the first time, a comprehensive picture of bilateral global migration over the last half of the twentieth century emerges. The data reveal that the global migrant stock increased from 92 to 165 million between 1960 and 2000. South-North migration is the fastest growing component of international migration in both absolute and relative terms. The United States remains the most important migrant destination in the world, home to one fifth of the world’s migrants and the top destination for migrants from no less than sixty sending countries. Migration to Western Europe remains largely from elsewhere in Europe. The oil-rich Persian Gulf countries emerge as important destinations for migrants from the Middle East, North Africa and South and South-East Asia. Finally, although the global migrant stock is still predominantly male, the proportion of women increased noticeably between 1960 and 2000.\" \"World Bank\" \"http://databank.worldbank.org/data/views/variableselection/selectvariables.aspx?source=global-bilateral-migration\" [\"World\" \"East Asia & Pacific\" \"Europe\" \" Asia\" \"Latin America\" \"Caribbean\" \"Middle East\" \"North Africa\" \"South Asia\" \"Sub Saharan\" \"Africa\"] \"1960 - 2000\" [\"migrants\" \"immigration\"] \"2011-07-01T00:00:00.000000\" \"2011-07-01T00:00:00.000000\" [] 5 0)"
  [title description publisher uri spatial temporal tags created updated resources reuses views]
  (list {:title title
         :description description
         :publisher publisher
         :uri uri
         :spatial spatial
         :temporal (extend-temporal temporal)
         :tags (vec (concat (tagify-title title)
                            tags))
         :created created
         :updated updated
         :resources resources
         :huntscore (calculate-huntscore reuses views 0 0)}))

;; (defn get-datagouvfr-ds
;;   "gets a number of the most popular datasets' metadata from the API of data.gouv.fr and transforms them to match the Hunter API scheme"
;;   [number]
;;   (let [response ((parse-string (:body (client/get (str base-url "datasets/?q=addiction&sort=-reuses"
;;                                                          "&page_size=" number))) true) :data)]
;;     (->> (map #(select-keys % [:title :page :description :last_modified :organization :spatial :tags :temporal_coverage :resources :metrics]) response)
;;          (map #(assoc %
;;                  :uri (% :page)
;;                  :publisher (if-not (nil? (get-in % [:organization :name]))
;;                               (get-in % [:organization :name])
;;                               "data.gouv.fr")
;;                  :spatial (if-not (empty? (get-spatial (get-in % [:spatial :territories])))
;;                             (get-spatial (get-in % [:spatial :territories]))
;;                             (geo-tagify "france"))
;;                  :tags (vec (concat (tagify-title (% :title))
;;                                     (extend-tags (% :tags))))
;;                  :description (if-not (empty? (% :description))
;;                                 (% :description)
;;                                 (% :title))
;;                  :temporal (if-not (empty? (% :temporal_coverage))
;;                              (extend-temporal (str (get-in % [:temporal_coverage :start])
;;                                                    "/"
;;                                                    (get-in % [:temporal_coverage :end])))
;;                              "all")
;;                  :created (if-not (nil? (get-in % [:resources 0 :created_at]))
;;                             (get-in % [:resources 0 :created_at])
;;                             (% :last_modified))
;;                  :updated (% :last_modified)
;;                  :resources (clean-resources (% :resources))
;;                  :huntscore (calculate-huntscore (get-in % [:metrics :reuses])
;;                                                  (get-in % [:metrics :views])
;;                                                  0
;;                                                  (get-in % [:metrics :followers]))))
;;          (map #(dissoc % :organization :temporal_coverage :page :metrics :last_modified)))))
