(ns ont-app.datascript-graph.path
  (:require
   [clojure.data.priority-map :as pm]
   [datascript.core           :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.datascript-graph.util :refer [cond-pred->]]
   [ont-app.igraph.core       :as igraph]))

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
  [dist-fn detect-neg? state {:keys [from to e]}]
  (let [alt-path (-> state
                     (get-in [:paths from])
                     (cond-pred-> ((complement nil?)) (conj e)))
        alt-dist (dist-fn alt-path)]
    (cond-pred->
     state
     (-> :paths
         to
         dist-fn
         (> alt-dist))
     ((fn [state]
        (when detect-neg? (throw (ex-info "Negative weight cycle" {})))
        (-> state
            (assoc-in [:paths to] alt-path)
            (assoc-in [:queue to] alt-dist)))))))

(defn dijkstra-shortest-path
  [_ state successor-fn dist-fn]
  (loop [from  (-> state
                   :paths
                   first)
         state (assoc state :queue (pm/priority-map))]
    (let [state (->> from
                     key
                     successor-fn
                     (reduce (partial maybe-swap dist-fn false) state))]
      (if (empty? (:queue state))
        state
        (recur (-> state
                   :queue
                   peek)
               (update state :queue pop))))))

(defn bellman-ford-shortest-path
  [g state successor-fn dist-fn & {:keys [skip-neg-detect?]}]
  (let [E (edges g successor-fn)
        n (-> g
              nodes
              count)]
    (-> n
        (cond-> skip-neg-detect? dec)
        range
        (->> (map (partial = (dec n)))
             (reduce (fn [state detect-neg?]
                       (->> E
                            (reduce (partial maybe-swap dist-fn detect-neg?)
                                    state)))
                     state)))))

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
        state        {:paths {source []}}
        f            (if (or detect-neg? (= alg ::bellman-ford))
                       bellman-ford-shortest-path
                       dijkstra-shortest-path)]
    (-> g
        (f state successor-fn dist-fn)
        :paths)))

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
