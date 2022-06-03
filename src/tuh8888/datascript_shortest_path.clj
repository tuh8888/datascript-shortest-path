(ns tuh8888.datascript-shortest-path
  (:require
   [datascript.core     :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.igraph.core :as igraph]))

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

(def graph-rules
  '[[(edge ?s ?p ?o ?e) [?e ::from ?s] [?e ::to ?o] [?e ::label ?p]]
    [(node? ?x) (or [_ ::from ?x] [_ ::to ?x])]])

(defn dijkstra-shortest-path-traversal
  [successor-fn dist-fn]
  (fn [g context acc [curr & queue]]
    (let [curr-path (get-in context [::paths curr] [])
          context   (->>
                     curr
                     (successor-fn g)
                     (reduce (fn [context [neighbor edge]]
                               (let [orig (get-in context [::paths neighbor])
                                     alt  (conj curr-path edge)]
                                 (cond-> context
                                   (->> [alt orig]
                                        (map (partial dist-fn g))
                                        (apply <))
                                   (assoc-in [::paths neighbor] alt))))
                             context))]
      [context
       (assoc acc curr curr-path)
       (sort-by (comp (partial dist-fn g) (::paths context)) queue)])))

(defn nodes
  [{:keys [db]}]
  (d/q '{:find  [[?id ...]]
         :in    [$ %]
         :where [(node? ?node) [?node ::dsg/id ?id]]}
       db
       graph-rules))

(defn shortest-path
  [g node successor-fn dist-fn]
  (let [acc       {}
        queue     (->> g
                       nodes
                       (into [node]))
        context   {::paths {node []}}
        traversal (dijkstra-shortest-path-traversal successor-fn dist-fn)]
    (igraph/traverse g traversal context acc queue)))
