(ns hunter-parser.ckan
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]))


;; (client/get
;; "https://catalog.data.gov/api/3/action/package_search?q=environmental")
