(ns tuh8888.datascript-shortest-path
  (:gen-class)
  (:require
   [datascript.core     :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.igraph.core :as igraph]))

(defn get-attr-id
  [subject predicate object]
  (->> [subject object]
       (map name)
       (apply format "%s-%s-%s" (random-uuid))
       (keyword (name predicate))))

(defn complex-triples
  [triples]
  (->> (for [[subject & p-os]         triples
             [predicate object-attrs] (partition 2 p-os)
             [object attrs]           object-attrs
             :let                     [attr-id (get-attr-id subject
                                                            predicate
                                                            object)]]
         (->> (for [[k v] attrs]
                [attr-id k v])
              (into [[subject predicate attr-id] [attr-id :target object]])))
       (apply concat)
       vec))

(defn edge-minimum-weight
  [db a b]
  (d/q '{:find  [(min ?dist) .]
         :in    [?ma ?mb $]
         :where [[?ma :to ?medge] [?medge :target ?mb] [?medge :weight ?dist]]}
       a
       b
       db))

(defn get-min-successors
  [g node]
  (d/q '{:find  [?b-id ?edge]
         :in    [$ ?a]
         :where [[?a :to ?edge]
                 [?edge :target ?b]
                 [(tuh8888.datascript-shortest-path/edge-minimum-weight $ ?a ?b)
                  ?mdist]
                 [?edge :weight ?mdist]
                 [?b ::dsg/id ?b-id]]}
       (:db g)
       [::dsg/id node]))

(defn calc-edge-dist
  [g edges]
  (if (nil? edges)
    Integer/MAX_VALUE
    (->> edges
         (d/pull-many (:db g) [:weight])
         (map :weight)
         (reduce + 0))))

(defn edges->node-path
  [g edges]
  (->> edges
       (d/pull-many (:db g) [{:target [::dsg/id]} :weight])
       (map #(update % :target (comp ::dsg/id first)))))

(defn dijkstra-shortest-path-traversal
  [link-fn dist-fn]
  (fn [g context acc [curr & queue]]
    (let [curr-path (get-in context [::paths curr] [])
          context   (->>
                     curr
                     (link-fn g)
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
  [g]
  (d/q '{:find  [[?id ...]]
         :where [(or [?node :to] [_ :target ?node])
                 [?node ::dsg/id ?id]
                 [?node ::dsg/id ?id]]}
       (:db g)))

(defn shortest-path
  [g
   node
   &
   {:keys [link-fn dist-fn]
    :or   {link-fn get-min-successors
           dist-fn calc-edge-dist}}]
  (let [acc       {}
        queue     (->> g
                       nodes
                       (into [node]))
        context   {::paths {node []}}
        traversal (dijkstra-shortest-path-traversal link-fn dist-fn)]
    (igraph/traverse g traversal context acc queue)))
