(ns ont-app.datascript-graph.path
  (:require
   [clojure.data.priority-map     :as pm]
   [mops.datascript-mops          :as dm]
   [ont-app.datascript-graph.fibonacci-heap :as fh]
   [ont-app.datascript-graph.util :refer [cond-pred->]]
   [ont-app.igraph.core           :as igraph]))

(defn edges
  [g successor-fn]
  (->> g
       dm/get-mops
       (mapcat successor-fn)))

(defn maybe-swap
  [update-state-fn path-fn dist-fn detect-neg? paths {:keys [from to e]}]
  (let [alt-path (-> paths
                     (path-fn from)
                     (cond-pred-> ((complement nil?)) (conj e)))
        alt-dist (dist-fn alt-path)]
    (cond-pred->
     paths
     (-> (path-fn to)
         dist-fn
         (> alt-dist))
     (-> (cond-> detect-neg? (do (throw (ex-info "Negative weight cycle" {}))))
         (update-state-fn to alt-path alt-dist)))))

(defn dijkstra-shortest-path
  [_ paths successor-fn dist-fn & {:keys [use-priority-map?]}]
  (let [update-state-fn (fn [state to alt-path alt-dist]
                          (-> state
                              (assoc-in [:queue
                                         to]
                                        alt-dist)
                              (assoc-in [:paths to] alt-path)))]
    (loop [state {:paths paths
                  :queue (assoc (if use-priority-map?
                                  (pm/priority-map)
                                  (fh/make-heap))
                                (-> paths
                                    keys
                                    first)
                                0)
                  :seen  #{}}]
      (let [from  (-> state
                      :queue
                      peek
                      key)
            state (update state :seen conj from)
            state (->> from
                       successor-fn
                       (remove (comp (:seen state) :to))
                       (reduce (partial maybe-swap
                                        update-state-fn
                                        (fn [state n]
                                          (-> state
                                              :paths
                                              n))
                                        dist-fn
                                        false)
                               (update state :queue pop)))]
        (if (-> state
                :queue
                empty?)
          (:paths state)
          (recur state))))))

(defn bellman-ford-shortest-path
  [g paths successor-fn dist-fn & {:keys [skip-neg-detect?]}]
  (let [E (edges g successor-fn)
        n (-> g
              dm/get-mops
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
                                             (fn [state n] (state n))
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
  (let [V         (dm/get-mops g)
        re-weight (let [g  (->> V
                                (reduce (fn [m to] (assoc m to {w-label 0})) {})
                                (vector ::s :to)
                                (dm/add-mop g))
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
