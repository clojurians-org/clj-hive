(ns oneonebang.hive.ql.udf
  (:import [org.apache.hadoop.hive.ql.udf.generic GenericUDF]
           [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspectorUtils ObjectInspectorFactory ObjectInspector ListObjectInspector StandardListObjectInspector
            primitive.PrimitiveObjectInspectorFactory
            PrimitiveObjectInspector$PrimitiveCategory])
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [oneonebang.hive.ql.common :as common]))

(gen-class :name oneonebang.hive.ql.udf.sum :prefix "sum-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn sum-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector))
(defn sum-evaluate [ this params ]
  (->> params (map #(common/force-lazy % :full)) seq (apply +)  common/writable) )
(defn sum-getDisplayString [ this params ] "sum")

(gen-class :name oneonebang.hive.ql.udf.json :prefix "json-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn json-initialize [this params ]
  (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/STRING))
(defn json-evaluate [ this params ]
  (-> params first (common/force-lazy :full) json/write-str common/writable) )
(defn json-getDisplayString [ this params ])

(gen-class :name oneonebang.hive.ql.udf.conj :prefix "conj-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn conj-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector) )
(defn conj-evaluate [ this params ]
  (->> params (map common/force-lazy) (apply concat)) )
(defn conj-getDisplayString [ this params ])

(gen-class :name oneonebang.hive.ql.udf.concat :prefix "concat-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn concat-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector) )
(defn concat-evaluate [ this params ]
  (->> params (map common/force-lazy) (apply concat)) )
(defn concat-getDisplayString [ this params ])

(gen-class :name oneonebang.hive.ql.udf.difference :prefix "difference-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn difference-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector) )
(defn difference-evaluate [ this params ]
  (->> params (map (comp set common/force-lazy)) (apply set/difference) vec) )
(defn difference-getDisplayString [ this params])
