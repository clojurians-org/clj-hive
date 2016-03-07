(ns oneonebang.hive.ql.common
  (:import [org.apache.hadoop.hive.ql.udf.generic GenericUDF$DeferredObject]
           [org.apache.hadoop.hive.serde2.lazybinary LazyBinaryArray LazyBinaryMap LazyBinaryStruct LazyBinaryPrimitive]
           [org.apache.hadoop.hive.serde2.lazy LazyArray LazyMap LazyStruct LazyPrimitive]
           [org.apache.hadoop.io Text IntWritable]
           [java.util List])
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [carbonite.api]
            [carbonite.buffer]))

(defn instance-some? [coll obj] (as-> obj $ #(instance? % $) (map $ coll) (some true? $)))

(defn eval-writable [obj]
  (cond
    (coll? obj) (cond->> (vec (map eval-writable obj)) (map? obj) (into {}))
    (instance? java.util.Map obj) (eval-writable (into {} obj))
    (instance-some? [java.util.List (Class/forName "[Ljava.lang.Object;")] obj) (eval-writable (into [] obj))
    (instance? Text obj) (.toString obj)
    (instance? IntWritable obj) (.get obj)
    :else obj))

(defn writable [obj]
  (cond
    (coll? obj) (cond->> (vec (map writable obj)) (map? obj) (into {}))
    (instance? java.util.Map obj) (writable (into {} obj))
    (instance-some? [java.util.List (Class/forName "[Ljava.lang.Object;")] obj) (writable (into [] obj))
    (number? obj) (IntWritable. obj)
    (string? obj) (Text. obj)
    :else obj))

(defn force-lazy
  ([lazy] (force-lazy lazy nil))
  ([lazy mode]
   (cond (instance-some? [LazyBinaryMap LazyMap] lazy) (force-lazy (into {} (.getMap lazy)))
         (instance-some? [LazyBinaryArray LazyArray] lazy) (force-lazy (into [] (.getList lazy)))
         (instance-some? [LazyBinaryStruct LazyStruct] lazy) (force-lazy (into [] (.getFieldsAsList lazy)))
         (instance-some? [LazyBinaryPrimitive LazyPrimitive] lazy) (force-lazy (.getWritableObject lazy) mode)
         (instance? GenericUDF$DeferredObject lazy) (force-lazy (.get lazy) mode)
         (= mode :full) (eval-writable lazy)
         :else lazy )))

(def registry (carbonite.api/default-registry))
(defn clone-obj [obj] (->> obj (carbonite.buffer/write-bytes registry) (carbonite.buffer/read-bytes registry)))

(defn get-in-arr-1 [coll k]
  (mapcat
    #(let [v (get % k)] (cond->> v ((complement sequential?) v) (conj [])))
    (cond->> coll ((complement sequential?) coll)  (conj [])) ))
(defn get-in-arr [coll ks]
  (reduce #(get-in-arr-1 %1 %2) [coll] ks))

(defn with-header [header m]
  (into {} (map (fn [[k v]] [(concat header (cond->> k ((complement sequential?) k) (conj []))) v]) m)))

(defn select-ks [obj ks]
  (cond (every? string? ks) (->> ks (mapv keyword) (select-keys obj) (with-header []))
        :else (let [[cursor ks] (split-with keyword? ks)
                    cur-obj (get-in obj cursor)
                    [ks-str ks-sequential] (split-with string? ks)
                    ks-str-map (->> (select-ks cur-obj ks-str)
                                    (with-header cursor))
                    ks-sequential-map (->> (mapv (partial select-ks cur-obj) ks-sequential)
                                       (apply merge)
                                       (with-header cursor))]
                (merge ks-str-map ks-sequential-map) )))
  
(defn flatten-json-1 [[prev-header prev-record-objs] [header & fields]]
  (let [cur-header  (concat prev-header header)]
    [cur-header
    (mapcat (fn [[prev-record prev-obj]]
              (for [cur-obj (get-in-arr prev-obj header)]
                [(as-> cur-obj $ (select-ks $ fields) (with-header cur-header $) (merge prev-record $) )
                cur-obj]))
            prev-record-objs)] ))
(defn flatten-json [m & ks]
  (as-> (reduce #(flatten-json-1 %1 %2) [nil [[{} m]]] ks) $
    (second $) (map first $)
    (map #(into {} (map (fn [[k v]] [(->> k (map name) (str/join "/")) v]) %)) $) ))
