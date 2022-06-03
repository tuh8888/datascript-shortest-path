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

(defn get-neighbors
  [g node]
  (->> g
       :db
       (d/q '{:find  [[(pull ?neighbor [::dsg/id]) ...]]
              :in    [?node $]
              :where [[?node :to ?edge] [?edge :target ?neighbor]]}
            [::dsg/id node])
       (map ::dsg/id)))

(defn edge-minimum-weight
  [db a b]
  (d/q '{:find  [(min ?dist) .]
         :in    [?ma ?mb $]
         :where [[?ma :to ?medge] [?medge :target ?mb] [?medge :weight ?dist]]}
       a
       b
       db))

(defn get-lowest-weight-edge
  [g a b]
  (->> g
       :db
       (d/q '{:find [?edge .]
              :in [?a ?b $]
              :where
              [[?a :to ?edge]
               [?edge :target ?b]
               [(tuh8888.datascript-shortest-path/edge-minimum-weight $ ?a ?b)
                ?mdist]
               [?edge :weight ?mdist]]}
            [::dsg/id a]
            [::dsg/id b])))

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
       (map (partial d/pull (:db g) [{:target [::dsg/id]} :weight]))
       (map #(update % :target (comp ::dsg/id first)))))

(defn dijkstra-shortest-path-traversal
  [neighbor-fn edge-fn dist-fn]
  (fn [g
       {::keys [paths]
        :as    context}
       acc
       [curr & queue]]
    (let [curr-path (get paths curr [])]
      (letfn [(update-path [paths neighbor]
                (update paths neighbor
                  (fn [path]
                    (let [alt-path (->> neighbor
                                        (edge-fn g curr)
                                        (conj curr-path))]
                      (if (->> [path alt-path]
                               (map (partial dist-fn g))
                               (apply <))
                        path
                        alt-path)))))
              (update-paths [paths]
                (->> curr
                     (neighbor-fn g)
                     (reduce update-path paths)))]
        (let [context (update context ::paths update-paths)]
          [context
           (assoc acc curr curr-path)
           (sort-by (comp (partial dist-fn g) (::paths context)) queue)])))))

(defn nodes
  [g]
  (->> g
       :db
       (d/q '{:find  [[(pull ?node [::dsg/id]) ...]]
              :where [(or [?node :to] [_ :target ?node])]})
       (map ::dsg/id)))

(defn shortest-path
  [g
   node
   &
   {:keys [neighbor-fn edge-fn dist-fn]
    :or   {neighbor-fn get-neighbors
           edge-fn     get-lowest-weight-edge
           dist-fn     calc-edge-dist}}]
  (let [acc       {}
        queue     (-> g
                      nodes
                      (conj node)
                      vec)
        context   {::paths {node []}}
        traversal (dijkstra-shortest-path-traversal neighbor-fn
                                                    edge-fn
                                                    dist-fn)]
    (igraph/traverse g traversal context acc queue)))
