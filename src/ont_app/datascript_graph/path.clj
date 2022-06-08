(ns ont-app.datascript-graph.path
  (:require
   [datascript.core           :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.datascript-graph.util :refer [cond-pred->]]
   [ont-app.igraph.core       :as igraph]
   [w01fe.fibonacci-heap.core :as fh]))

(def graph-rules
  '[[(edge ?s ?p ?o ?e) [?e ::from ?s] [?e ::to ?o] [?e ::label ?p]]
    [(node? ?x) (or [_ ::from ?x] [_ ::to ?x])]
    [(edge? ?x) (or [?x ::from _] [?x ::to _] [?x ::label _])]])

(defn nodes
  [{:keys [db]}]
  (d/q '{:find  [[?id ...]]
         :in    [$ %]
         :where [(node? ?node) [?node ::dsg/id ?id]]}
       db
       graph-rules))

(defn edges
  [g successor-fn]
  (->> g
       nodes
       (mapcat successor-fn)))

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

(defn maybe-swap
  [update-state-fn dist-fn detect-neg? paths {:keys [from to e]}]
  (let [alt-path (-> paths
                     from
                     (cond-pred-> ((complement nil?)) (conj e)))
        alt-dist (dist-fn alt-path)]
    (cond-pred->
     paths
     (-> to
         dist-fn
         (> alt-dist))
     (-> (cond-> detect-neg? (do (throw (ex-info "Negative weight cycle" {}))))
         (update-state-fn to alt-path alt-dist)))))

(defn dijkstra-shortest-path
  [g paths successor-fn dist-fn]
  (let [h               (fh/fibonacci-heap)
        h-nodes         (->> g
                             nodes
                             (reduce (fn [m n]
                                       (->> n
                                            (fh/add! h Integer/MAX_VALUE)
                                            (assoc m n)))
                                     {}))
        dec-key!        (fn [k v] (fh/decrease-key! h (h-nodes k) (int v) k))
        update-state-fn (fn [state to alt-path alt-dist]
                          (dec-key! to alt-dist)
                          (assoc state to alt-path))
        source          (-> paths
                            keys
                            first)]
    (dec-key! source 0)
    (loop [paths paths]
      (let [from  (second (fh/remove-min! h))
            state (->> from
                       successor-fn
                       (reduce (partial maybe-swap
                                        update-state-fn
                                        dist-fn
                                        false)
                               paths))]
        (if (fh/empty? h) state (recur state))))))

(defn bellman-ford-shortest-path
  [g paths successor-fn dist-fn & {:keys [skip-neg-detect?]}]
  (let [E (edges g successor-fn)
        n (-> g
              nodes
              count)]
    (-> n
        (cond-> skip-neg-detect? dec)
        range
        (->> (map (partial = (dec n)))
             (reduce (fn [paths detect-neg?]
                       (->> E
                            (reduce (partial maybe-swap
                                             (fn [state to alt-path _]
                                               (assoc state to alt-path))
                                             dist-fn
                                             detect-neg?)
                                    paths)))
                     paths)))))

(defn shortest-path
  [g
   source
   successor-fn
   dist-fn
   &
   {:keys [alg detect-neg?]
    :or   {alg ::dijkstra}}]
  (let [dist-fn      (partial dist-fn g)
        successor-fn (partial successor-fn g)
        paths        {source []}
        f            (if (or detect-neg? (= alg ::bellman-ford))
                       bellman-ford-shortest-path
                       dijkstra-shortest-path)]
    (-> g
        (f paths successor-fn dist-fn))))

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
                        bf (shortest-path g
                                          ::s
                                          successor-fn
                                          dist-fn
                                          :alg                ::bellman-ford
                                          :detect-neg-cycles? true)]
                    (letfn [(h [node] (dist-fn g (get bf node)))]
                      (fn [a b weight] (- (+ weight (h a)) (h b)))))
        g         (->> (edges g (partial successor-fn g))
                       (reduce (fn [coll {:keys [from to id e]}]
                                 (->> [e]
                                      (dist-fn g)
                                      (re-weight from to)
                                      (vector id w-label)
                                      (conj coll)))
                               [])
                       (igraph/add g))]
    (reduce
     (fn [paths source]
       (assoc paths source (shortest-path g source successor-fn dist-fn)))
     {}
     V)))
