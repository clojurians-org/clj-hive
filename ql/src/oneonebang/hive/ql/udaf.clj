(ns oneonebang.hive.ql.udaf
  (:import [org.apache.hadoop.hive.ql.exec UDF]
           [org.apache.hadoop.hive.ql.udf.generic GenericUDAFEvaluator GenericUDAFEvaluator$Mode]
           [org.apache.hadoop.hive.ql.udf.generic GenericUDF GenericUDF$DeferredObject]
           [org.apache.hadoop.hive.serde2.objectinspector
            ObjectInspectorUtils ObjectInspectorFactory ObjectInspector ListObjectInspector StandardListObjectInspector]
           [org.apache.hadoop.hive.serde2.lazybinary LazyBinaryArray LazyBinaryMap LazyBinaryStruct LazyBinaryPrimitive]
           [org.apache.hadoop.hive.serde2.lazy LazyArray LazyMap LazyStruct LazyPrimitive]
           [org.apache.hadoop.io Text IntWritable]
           [java.util List])
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [oneonebang.hive.ql.common :as common]))

(gen-class :name oneonebang.hive.ql.udaf.buffer :state "state" :init "buffer-init"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator$AbstractAggregationBuffer)
(defn -buffer-init [] [[] (atom nil)])

(gen-class :name oneonebang.hive.ql.udaf.collect.evaluator :exposes-methods {init pinit} :prefix "collect-evaluator-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator)
(defn collect-evaluator-getNewAggregationBuffer [this] (oneonebang.hive.ql.udaf.buffer.))
(defn collect-evaluator-reset [this agg] (reset! (.state agg) nil))
(defn collect-evaluator-terminate [this agg] @(.state agg))
(defn collect-evaluator-terminatePartial [this agg] @(.state agg))

(defn collect-evaluator-init [this mode params]
  (.pinit this mode params)
  (cond (= GenericUDAFEvaluator$Mode/PARTIAL1 mode)
        (-> params first ObjectInspectorUtils/getStandardObjectInspector ObjectInspectorFactory/getStandardListObjectInspector)
        :else (-> params first ObjectInspectorUtils/getStandardObjectInspector)))
(defn collect-evaluator-merge [this agg xpartial]
  (when-not @(.state agg) (reset! (.state agg) []))
  (swap! (.state agg) concat (common/force-lazy xpartial)) )
(defn collect-evaluator-iterate [this agg params] (collect-evaluator-merge this agg params))

(gen-class :name oneonebang.hive.ql.udaf.collect :prefix "collect-"
           :extends org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver)
(defn collect-getEvaluator [ this type-info-params ]  (oneonebang.hive.ql.udaf.collect.evaluator.))

(gen-class :name oneonebang.hive.ql.udaf.merge_with.evaluator :exposes-methods {init pinit} :prefix "merge-with-evaluator-"
           :extends org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator)
(defn merge-with-evaluator-getNewAggregationBuffer [this] (oneonebang.hive.ql.udaf.buffer.))
(defn merge-with-evaluator-reset [this agg] (reset! (.state agg) nil))

(defn merge-with-evaluator-terminate [this agg] (second @(.state agg)))
(defn merge-with-evaluator-terminatePartial [this agg] @(.state agg))
(defn merge-with-evaluator-init [this mode params]
  (.pinit this mode params)
  (cond (= GenericUDAFEvaluator$Mode/PARTIAL1 mode)
          (ObjectInspectorFactory/getStandardStructObjectInspector ["fn" "map"]
              (map #(ObjectInspectorUtils/getStandardObjectInspector %) params))
          :else (-> params first .getAllStructFieldRefs second .getFieldObjectInspector ObjectInspectorUtils/getStandardObjectInspector) ))
(defn merge-with-evaluator-merge [this agg xpartial]
  (when-not @(.state agg) (reset! (.state agg) [nil {}]))
  (let [[fn-str amap] (-> xpartial (common/force-lazy :full) common/clone-obj)
        xfn ((comp eval read-string) fn-str)]
    (reset! (.state agg) (common/writable [fn-str (merge-with xfn (second (common/eval-writable @(.state agg))) amap)])) ))
(defn merge-with-evaluator-iterate [this agg params]
  (let [[fn-str amap] (as-> params $ (map #(-> % (common/force-lazy :full) common/clone-obj) $))]
    (merge-with-evaluator-merge this agg [fn-str amap]) ))
(gen-class :name oneonebang.hive.ql.udaf.merge_with :prefix "merge-with-"
           :extends org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver)
(defn merge-with-getEvaluator [ this type-info-params ]  (oneonebang.hive.ql.udaf.merge_with.evaluator.))
