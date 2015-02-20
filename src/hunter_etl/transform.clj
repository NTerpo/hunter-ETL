(ns hunter-etl.transform
  (:use [clojure.data :refer :all])
  (:require [hunter-etl.util :refer :all]))

;;;; Keys - Hunter API Scheme

(def hunter-keys
  "keys needed in the Hunter API
  [:title :description :publisher
  :uri :created :updated :spatial
  :temporal :tags :resources :huntscore]"
  [:title :description :publisher :uri :created :updated :spatial :temporal :tags :resources :huntscore])

(defn not-hunter-keys
  "keys on a collection that are not on the hunter-keys"
  [vect]
  (vec
   (second
    (diff (set hunter-keys) (set vect)))))

;;;; By Hand Datasets

(defn create-ds
  "create a hunter dataset by hand
  eg.
  (create-ds \"Global Bilateral Migration Database\"
  \"Global ...\"  \"World Bank\" \"http://databank.w...\"
  [\"World\" \"Africa\"] \"1960 - 2000\"
  [\"migrants\"] \"2011-07-01T00:00:00.000000\"
  \"2011-07-01T00:00:00.000000\" [] 5 0)"
  [title description publisher uri spatial temporal
   tags created updated resources reuses views]
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

;;;; Transformation Macro

(defmacro map-get-in
  "Given a map and a vector of key [f :key1 :key2]
  returns (f (get-in map [:key1]) (get-in map [:key2]))

  Used in the deftransform macro to avoid redundancy"
  [m coll]
  (let [args# `(map #(get-in ~m (if (vector? %) % [%]))
                      (rest ~coll))]
    (list apply `(first ~coll) args#)))

(defmacro with-count-check
  "Macro that checks the count of a vector.
  If the count is 1, it returns the element, if not it
  returns the body.

  Used in deftransform to allow to pass transformation
  in two different ways:
  * :key [ƒ :key1 :key2]
  * :key ['value']"
  [v & body]
  `(if (= 1 (count ~v))
    (first ~v)
    (do ~@body)))

(defmacro deftransform
  "Define a function that transform a collection of
  dataset's metadata to make it meet the Hunter API
  scheme.

  The first argument is the name of the returned function

  The second one is an array of the keys present in
  the given collection of hashmaps and needed

  The third is a map {:filter boolean?} (or {}) used to
  filter the collection (filter the unpublished dataset
  for exemple)

  Then it takes a map of pairs :hunter-key [ƒ :old-key]
  each function returns the final value of each key

  e.g.
  (deftransform dgf-transform
  
  [:title :page :description :last_modified :organization
   :spatial :tags :temporal_coverage :resources :metrics]

  {}
  
  {:title       [identity :title]
   :description [notes->description :description :title]
   :publisher   [\"foo\"]
   :uri         [url->uri :page]
   :created     [get-created [:resources 0 :created_at] :last_modified]
   :updated     [identity :last_modified]
   :spatial     [(geo-tagify \"us\"]]
   :temporal    [get-temporal [:temporal_coverage :start] [:temporal_coverage :end]]
   :tags        [tags-with-title :title :tags]
   :resources   [filter-resources :resources]
   :huntscore   [str [:metrics :reuses] [:metrics :views]]})"
  [name keys boolean fns-map]
  {:pre [(and (vector? keys) (map? boolean) (map? fns-map))]}
  (cons 'defn
        `(~name "Pipeline to transform the collection received from the API
  and make it meet the Hunter API scheme."
                [coll#]
                (let [ks# ~keys 
                      nks# (not-hunter-keys ks#)
                      {title-t# :title description-t# :description
                       publisher-t# :publisher uri-t# :uri
                       created-t# :created updated-t# :updated
                       spatial-t# :spatial temporal-t# :temporal
                       tags-t# :tags resources-t# :resources
                       huntscore-t# :huntscore} ~fns-map
                      {bool# :filter :or {bool# identity}} ~boolean]

                  (->> coll#
                       (filter bool#)
                       (map #(select-keys % ks#))
                       (map #(assoc %
                               :title (with-count-check title-t# (map-get-in % title-t#))
                               :description (with-count-check description-t# (map-get-in % description-t#))
                               :publisher (with-count-check publisher-t# (map-get-in % publisher-t#))
                               :uri (with-count-check uri-t# (map-get-in % uri-t#))
                               :created (with-count-check created-t# (map-get-in % created-t#))
                               :updated (with-count-check updated-t# (map-get-in % updated-t#))
                               :spatial (with-count-check spatial-t# (map-get-in % spatial-t#))
                               :temporal (with-count-check temporal-t# (map-get-in % temporal-t#))
                               :tags (with-count-check tags-t# (map-get-in % tags-t#))
                               :resources (with-count-check resources-t# (map-get-in % resources-t#))
                               :huntscore (with-count-check huntscore-t# (map-get-in % huntscore-t#))))
                       (map #(apply dissoc % nks#)))))))


