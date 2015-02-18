# Hunter ETL

 * Extract data from different Open Data portals API
 * Transform them to meet the Hunter API scheme
 * Load them to the Hunter DB, via the API

## Usage

### Extract

    (defn dg-extract
      "extract data from the data.gov API and clean the introduction
      returns a collection of datasets metadata"
      [& args]
      (apply extract-from-ckan-v3 dg-url args))

### Transform

    (deftransform dg-transform
      [:title :notes :organization :resources :tags :extras
       :revision_timestamp :tracking_summary]

      {:filter published?}

      {:title       [identity :title]
       :description [notes->description :notes :title]
       :publisher   [identity [:organization :title]]
       :uri         [url->uri [:resources 0 :url]]
       :created     [get-created [:resources 0 :created] :revision_timestamp]
       :updated     [identity :revision_timestamp]
       :tags        [tags-with-title :title :tags]
       :spatial     [(geo-tagify "us")]
       :temporal    [get-temporal :extras]
       :resources   [clean-resources :resources :title]
       :huntscore   [dg-huntscore [:tracking_summary :recent] [:tracking_summary :total]]})
    
### Load

#### Hunter API

* `$ mongod`
* `cd path/to/hunter-api`
* edit hunter-api/src/hunter_api/data.clj:

    (def config ;; used in development
      {:conn (connect {:host "localhost" :port 27017})
       :db (get-db (connect {:host "localhost" :port 27017}) "update-here")
       :db-name "update-here"})

* `$ lein ring server`

#### Run ETL

    (defn dg-etl
      "data.gov ETL
      takes between 0 and 3 arguments :
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

## License

Copyright © 2015 Nicolas TERPOLILLI

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
