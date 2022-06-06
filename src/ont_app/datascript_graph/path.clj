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
  [g successor-fn]
  (for [from   (nodes g)
        [to e] (successor-fn g from)]
    {:from from
     :to   to
     :e    e
     :id   (::dsg/id (d/pull (:db g) [::dsg/id] e))}))

(defn ->edge-ref
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
         (let [e  (->edge-ref s p o)
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
    (let [curr-path (get paths curr)
          acc       (cond-> acc
                      curr-path (assoc curr curr-path))
          paths     (->>
                     curr
                     (successor-fn g)
                     (reduce
                      (fn [paths [neighbor edge]]
                        (let [alt (-> curr-path
                                      (cond-pred-> ((complement nil?))
                                                   (conj edge)))]
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
  [g source successor-fn dist-fn & {:keys [detect-neg-cycles?]}]
  (let [E         (edges g successor-fn)
        num-nodes (-> g
                      nodes
                      count)]
    (-> num-nodes
        (cond-> (not detect-neg-cycles?) dec)
        range
        (->>
         (reduce
          (fn [paths i]
            (reduce (fn [paths {:keys [from to e]}]
                      (let [alt (-> paths
                                    (get from)
                                    (cond-pred-> ((complement nil?)) (conj e)))]
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

(defn johnson-all-pairs-shortest-paths
  "Finds the shortest paths between all pairs of nodes using Johnson's algorithm."
  [g successor-fn dist-fn w-label]
  (let [V         (nodes g)
        re-weight (let [g  (->> V
                                (reduce (fn [m to] (assoc m to {w-label 0})) {})
                                (vector ::s :to)
                                vector
                                complex-triples
                                (igraph/add g))
                        bf (bellman-ford-shortest-path g
                                                       ::s
                                                       successor-fn
                                                       dist-fn
                                                       :detect-neg-cycles?
                                                       true)]
                    (letfn [(h [node] (dist-fn g (get bf node)))]
                      (fn [a b weight] (- (+ weight (h a)) (h b)))))
        g         (->> (edges g successor-fn)
                       (reduce (fn [coll {:keys [from to id e]}]
                                 (->> [e]
                                      (dist-fn g)
                                      (re-weight from to)
                                      (vector id w-label)
                                      (conj coll)))
                               [])
                       (igraph/add g))]
    (reduce (fn [paths source]
              (assoc paths
                     source
                     (dijkstra-shortest-path g source successor-fn dist-fn)))
            {}
            V)))
