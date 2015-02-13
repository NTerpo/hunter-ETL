(ns hunter-etl.ckan
  (:require [hunter-etl.util :refer :all]))

(defn extract-from-ckan
  ([base-api] (extract-from-ckan base-api 1 0 ""))
  
  ([base-api number] (extract-from-ckan base-api number 0 ""))

  ([base-api number offset] (extract-from-ckan base-api number offset ""))
  
  ([base-api number offset request]
   (((get-result (str base-api "/3/" "action/package_search?"
                      "q=" request
                      "&rows=" number
                      "&start=" offset))
     :result)
    :results)))
