(ns oneonebang.hive.ql.udtf
  (:import [org.apache.hadoop.hive.ql.udf.generic GenericUDTF]
           [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspectorUtils ObjectInspectorFactory ObjectInspector ListObjectInspector StandardListObjectInspector
            primitive.PrimitiveObjectInspectorFactory
            PrimitiveObjectInspector$PrimitiveCategory])
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [oneonebang.hive.ql.common :as common]))

(gen-class :name oneonebang.hive.ql.udtf.explode_json :prefix "explode-json-" :exposes-methods {forward pforward}
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDTF)
(defn explode-json-close [this])
(defn explode-json-initialize [ this params ]
  (ObjectInspectorFactory/getStandardStructObjectInspector ["pos" "map"]
    [(PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/INT)
     (ObjectInspectorFactory/getStandardMapObjectInspector
      (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/STRING)
      (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/STRING))]))
(defn explode-json-process [ this params ]
  (let [[json-str json-expr-str] (map #(common/force-lazy % :full) params)
        [json-obj json-expr]     [(json/read-str json-str :key-fn keyword) (read-string json-expr-str)]
        explode-json-obj  (as-> (apply common/flatten-json json-obj json-expr) $
                            (map-indexed #(do [%1 (into {} (map (fn [[k v]] [k (str v)]) %2))]) $)) ]
    (doseq [record explode-json-obj] (.pforward this (common/writable record))) ))
(defn explode-json-toString [ this ] "explode-json")

