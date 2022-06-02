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
       (apply format "%s-%s")
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

(defn get-weight
  [g a b]
  (->> g
       :db
       (d/q '{:find  [?dist .]
              :in    [?a ?b $]
              :where [[?a :to ?edge] [?edge :target ?b] [?edge :weight ?dist]]}
            [::dsg/id a]
            [::dsg/id b])))

(defn dijkstra-shortest-path-traversal
  [neighbor-fn weight-fn]
  (fn [g
       {::keys [paths]
        :as    context}
       acc
       [curr & queue]]
    (letfn
      [(update-path [curr-path paths neighbor weight]
         (update paths neighbor
           (fn [path]
             (let [alt-dist (+ weight (::dist curr-path))]
               (if (and path (< (::dist path) alt-dist))
                 path
                 (assoc curr-path ::dist alt-dist))))))
       (update-paths [paths]
         (let [paths (update-in paths [curr ::nodes] (fnil conj []) curr)]
           (->> curr
                (neighbor-fn g)
                (map (juxt identity (partial weight-fn g curr)))
                (into {})
                (reduce-kv (partial update-path (get paths curr)) paths))))]
      (let [paths (update-paths paths)]
        [(assoc context ::paths paths)
         (assoc acc curr (get paths curr))
         (sort-by (some-fn #(get-in paths [% ::dist])
                           (constantly Integer/MAX_VALUE))
                  queue)]))))

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
   {:keys [neighbor-fn weight-fn]
    :or   {neighbor-fn get-neighbors
           weight-fn   get-weight}}]
  (let [acc       {}
        queue     (-> g
                      nodes
                      (conj node)
                      vec)
        context   {::paths {node {::dist 0}}}
        traversal (dijkstra-shortest-path-traversal neighbor-fn weight-fn)]
    (igraph/traverse g traversal context acc queue)))
