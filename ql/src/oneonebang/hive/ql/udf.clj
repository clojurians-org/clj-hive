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
  (let [[coll & elements] (->> params (map common/force-lazy))]
        (apply conj (seq coll) elements) ))
(defn conj-getDisplayString [ this params ])

(gen-class :name oneonebang.hive.ql.udf.concat :prefix "concat-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn concat-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector) )
(defn concat-evaluate [ this params ]
  (->> params (map common/force-lazy) (apply concat)) )
(defn concat-getDisplayString [ this params ])

(gen-class :name oneonebang.hive.ql.udf.str :prefix "str-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn str-initialize [ this params ]
  (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/STRING))
(defn str-evaluate [ this params ]
  (->> params (map common/force-lazy) (apply str) common/writable) )
(defn str-getDisplayString [ this params ] "str")

(gen-class :name oneonebang.hive.ql.udf.difference :prefix "difference-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn difference-initialize [ this params ]
  (-> params first ObjectInspectorUtils/getStandardObjectInspector) )
(defn difference-evaluate [ this params ]
  (->> params (map (comp set common/force-lazy)) (apply set/difference) vec) )
(defn difference-getDisplayString [ this params])

(gen-class :name oneonebang.hive.ql.udf.merge_with :prefix "merge-with-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn merge-with-initialize [ this params ]
  (-> params second ObjectInspectorUtils/getStandardObjectInspector) )
(defn merge-with-evaluate [ this params ]
  (let [[fn-str & final-params] (->> params (map #(common/force-lazy % :full)))]
        (common/writable (apply merge-with (-> fn-str read-string eval) final-params)) ))
(defn merge-with-getDisplayString [ this params ] "merge-with")

(gen-class :name oneonebang.hive.ql.udf.filter :prefix "filter-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn filter-initialize [ this params ]
  (-> params second ObjectInspectorUtils/getStandardObjectInspector))
(defn filter-evaluate [ this params ]
  (let [[fn-str coll] (->> params (map #(common/force-lazy % :full)))]
    (common/writable (filter (-> fn-str read-string eval) coll)) ))
(defn filter-getDisplayString [ this params ] "filter")

(gen-class :name oneonebang.hive.ql.udf.map :prefix "map-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn map-initialize [ this params ]
  (-> params second ObjectInspectorUtils/getStandardObjectInspector))
(defn map-evaluate [ this params ]
  (let [[fn-str coll] (->> params (map #(common/force-lazy % :full)))
        map-ret (map (-> fn-str read-string eval) coll)]
    (common/writable (cond->> map-ret (map? coll) (into {}))) ))
(defn map-getDisplayString [ this params ] "map")

(gen-class :name oneonebang.hive.ql.udf.frequencies :prefix "frequencies-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn frequencies-initialize [ this params ]
  (ObjectInspectorFactory/getStandardMapObjectInspector
   (-> params first .getListElementObjectInspector ObjectInspectorUtils/getStandardObjectInspector)
   (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/INT) ))
(defn frequencies-evaluate [ this params ]
  (->> params first  common/force-lazy frequencies common/writable) )
(defn frequencies-getDisplayString [ this params ] "frequencies")

(gen-class :name oneonebang.hive.ql.udf.distinct :prefix "distinct-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn distinct-initialize [ this params ]
   (-> params first ObjectInspectorUtils/getStandardObjectInspector))
(defn distinct-evaluate [ this params ]
  (->> params first  common/force-lazy distinct common/writable) )
(defn distinct-getDisplayString [ this params ] "distinct")

(gen-class :name oneonebang.hive.ql.udf.count :prefix "count-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
(defn count-initialize [ this params ]
  (PrimitiveObjectInspectorFactory/getPrimitiveWritableObjectInspector PrimitiveObjectInspector$PrimitiveCategory/INT))
(defn count-evaluate [ this params ]
  (-> params first  (common/force-lazy :full) count common/writable) )
(defn count-getDisplayString [ this params ] "distinct")

(defmacro gen-tfn [fn-name]
  (let [fn-class-name (str "t" (str/replace fn-name "-" "_"))]
  `(do
     (gen-class  :name ~(read-string (str "oneonebang.hive.ql.udf." fn-class-name)) :prefix ~(str "t" fn-name "-")
                 :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDF)
     (defn ~(read-string (str "t" fn-name "-initialize")) [this# params#]
       (~(read-string (str fn-name "-initialize")) this# (conj (-> params# drop-last vec) (.getListElementObjectInspector (last params#)))) )
     (defn ~(read-string (str "t" fn-name "-evaluate")) [this# params#]
       (~(read-string (str fn-name "-evaluate")) this# (let [param-objs# (map common/force-lazy params#)] (apply conj (vec (drop-last param-objs#)) (last param-objs#))  )))
     (defn ~(read-string (str "t" fn-name "-getDisplayString")) [this# params#] ~(str "t" fn-name)) ) ))
(gen-tfn "sum")
(gen-tfn "concat")
