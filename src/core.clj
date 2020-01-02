(ns core
  (:require [crux.api :as api]))

(defn dedupe-fn [f coll]
  (lazy-seq
    (if-let [y (second coll)]
      (let [x (first coll)]
        (let [xs (seq (rest coll))]
          (if (f x y)
            (cons y (dedupe-fn f (rest xs)))
            (cons x (dedupe-fn f xs)))))
      coll)))

(defn query-descending [node query]
  (let [db (api/db node)
        snapshot (api/new-snapshot db)
        doc-es (into #{} (map first (:where query)))
        doc-ids (map first (api/q db {:find (vec doc-es) :where (:where query)}))
        doc-times (sort (flatten (map #(map :crux.db/valid-time (api/history-descending db snapshot %)) doc-ids)))
        query-hist (map (fn [vtime] [vtime (api/q (api/db node vtime) query)]) doc-times)]
    (dedupe-fn (fn [x y] (= (second x) (second y))) query-hist)))
