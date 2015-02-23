(ns hunter-etl.core
  (:require [hunter-etl.load :refer [load-to-hunter-api]]
            [hunter-etl.portals.data-gov :refer [dg-extract dg-transform]]
            [hunter-etl.portals.data-gouv-fr :refer [dgf-extract dgf-transform]]
            [hunter-etl.portals.data-gov-uk :refer [dguk-extract dguk-transform]]
            [hunter-etl.portals.world-bank-data :refer [wb-extract wb-transform]]
            [hunter-etl.portals.open-canada :refer [ca-extract ca-transform]]
            [hunter-etl.portals.energy-data-usa :refer [edex-extract edex-transform]]))

(defn dg-etl
  "data.gov ETL
  takes between 0 and 3 arguments:
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an offset
  3 => same but only with datasets corresponding to a query

  Works perfectly well with number = 1000"
  [& args]
  (-> (apply dg-extract args)
      dg-transform
      load-to-hunter-api))

(defn dgf-etl
  "data.gouv.fr ETL
  takes between 0 and 3 arguments:
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an page number ~ offset= page*number
  3 => same but only with datasets corresponding to a query

  With a number argument too big - eg 1000 - gets a HTTP 500 from the API"
  [& args]
  (-> (apply dgf-extract args)
      dgf-transform
      load-to-hunter-api))

(defn dguk-etl
  "data.gov.uk ETL
  takes between 0 and 3 arguments:
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an offset
  3 => same but only with datasets corresponding to a query"
  [& args]
  (-> (apply dguk-extract args)
      dguk-transform
      load-to-hunter-api))

(defn wb-etl
  "World Bank Data ETL
  takes between 0 and 2 arguments:
  ([integer < 120] [integer])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an page number ~ offset= page*number"
  [& args]
  (-> (apply wb-extract args)
      wb-transform
      load-to-hunter-api))

(defn ca-etl
  "open.canada.ca ETL
  takes between 0 and 2 arguments:
  ([integer] [integer])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an page number ~ offset= page*number"
  [number offset]
  (-> (ca-extract number offset)
      ca-transform
      load-to-hunter-api))

(defn edex-etl
  "Energy Data EXchange USA ETL
  takes between 0 and 3 arguments:
  ([integer][integer][string])
  
  0 => loads in the Hunter DB the most popular dgu dataset cleaned
  1 => loads in the Hunter DB the given number of dgu datasets cleaned
  2 => same with an offset
  3 => same but only with datasets corresponding to a query"
  [& args]
  (-> (apply edex-extract args)
      edex-transform
      load-to-hunter-api))
