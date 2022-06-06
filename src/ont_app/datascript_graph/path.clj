(ns ont-app.datascript-graph.path
  (:require
   [datascript.core     :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.igraph.core :as igraph]))

(def graph-rules
  '[[(edge ?s ?p ?o ?e) [?e ::from ?s] [?e ::to ?o] [?e ::label ?p]]
    [(node? ?x) (or [_ ::from ?x] [_ ::to ?x])]
    [(edge? ?x) (or [?x ::from _] [?x ::to _] [?x ::label _])]])

(defmacro cond-pred->
  "Like cond-> but also threads initial expr through tests."
  {:added "1.5"}
  [expr & clauses]
  (assert (even? (count clauses)))
  (let [g     (gensym)
        steps (map (fn [[test step]]
                     `(if (-> ~g
                              ~test)
                        (-> ~g
                            ~step)
                        ~g))
                   (partition 2 clauses))]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps) g (last steps)))))

(defn nodes
  [{:keys [db]}]
  (d/q '{:find  [[?id ...]]
         :in    [$ %]
         :where [(node? ?node) [?node ::dsg/id ?id]]}
       db
       graph-rules))

(defn edges
  [{:keys [db]}]
  (d/q '{:find  [?from-id ?to-id ?e]
         :keys  [from to edge]
         :in    [$ %]
         :where [(edge ?from _ ?to ?e)
                 [?from ::dsg/id ?from-id]
                 [?to ::dsg/id ?to-id]]}
       db
       graph-rules))

(defn ->edge-id
  [subject predicate object]
  (->> [subject object]
       (map name)
       (apply format "%s-%s-%s" (random-uuid))
       (keyword (name predicate))))

(defn complex-triples
  [triples]
  (->> (for [[s & p-os]       triples
             [p object-attrs] (partition 2 p-os)
             [o attrs]        object-attrs]
         (let [e  (->edge-id s p o)
               ts [[e ::from s] [e ::to o] [e ::label p]]]
           (->> attrs
                (map (partial into [e]))
                (into ts))))
       (reduce into [])))

(defn further? [b a dist-fn] (< (dist-fn a) (dist-fn b)))

(defn dijkstra-traversal
  [successor-fn dist-fn]
  (fn [g
       {::keys [paths]
        :as    context}
       acc
       [curr & queue]]
    (let [curr-path (get paths curr [])
          acc       (assoc acc curr curr-path)
          paths     (->>
                     curr
                     (successor-fn g)
                     (reduce
                      (fn [paths [neighbor edge]]
                        (let [alt (conj curr-path edge)]
                          (cond-pred-> paths
                                       (-> neighbor
                                           (further? alt (partial dist-fn g)))
                                       (assoc neighbor alt))))
                      paths))
          queue     (sort-by (comp (partial dist-fn g) paths) queue)
          context   (assoc context ::paths paths)]
      [context acc queue])))

(defn dijkstra-shortest-path
  [g source successor-fn dist-fn]
  (let [acc       {}
        queue     (->> g
                       nodes
                       (into [source]))
        context   {::paths {source []}}
        traversal (dijkstra-traversal successor-fn dist-fn)]
    (igraph/traverse g traversal context acc queue)))

(defn bellman-ford-shortest-path
  [g source dist-fn & {:keys [detect-neg-cycles?]}]
  (let [E         (edges g)
        num-nodes (-> g
                      nodes
                      count)]
    (-> num-nodes
        (cond-> (not detect-neg-cycles?) dec)
        range
        (->> (reduce
              (fn [paths i]
                (reduce
                 (fn [paths {:keys [from to edge]}]
                   (let [alt (-> paths
                                 (get from)
                                 (cond-pred-> ((complement nil?)) (conj edge)))]
                     (cond-pred->
                      paths
                      (-> to
                          (further? alt (partial dist-fn g)))
                      (assoc to
                             (if (= (dec num-nodes) i)
                               (throw (ex-info "Negative weight cycle" {}))
                               alt)))))
                 paths
                 E))
              {source []})))))
