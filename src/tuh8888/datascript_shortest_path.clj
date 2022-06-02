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
  (->> (d/q '{:find  [[(pull ?neighbor [::dsg/id]) ...]]
              :in    [$ ?node]
              :where [[?node :to ?edge] [?edge :target ?neighbor]]}
            (:db g)
            [::dsg/id node])
       (map ::dsg/id)))

(defn get-weight
  [g a b]
  (d/q '{:find  [?dist .]
         :in    [$ ?a ?b]
         :where [[?a :to ?edge] [?edge :target ?b] [?edge :weight ?dist]]}
       (:db g)
       [::dsg/id a]
       [::dsg/id b]))

(defn dijkstra-shortest-path-traversal
  [neighbor-fn weight-fn]
  (letfn
    [(update-path [{::keys [dist]
                    :as    path} g neighbor current current-path]
       (let [dist (::dist path)
             alt  (->> neighbor
                       (weight-fn g current)
                       (+ (::dist current-path)))]
         (if (and dist (< dist alt))
           path
           {::dist  alt
            ::nodes (::nodes current-path)})))
     (update-paths [paths g current current-path]
       (->>
        current
        (neighbor-fn g)
        (reduce
         (fn [paths neighbor]
           (update paths neighbor update-path g neighbor current current-path))
         paths)))]
    (fn [g context acc [current & queue]]
      (let [current-path (-> context
                             (get-in [::paths current])
                             (update ::nodes (fnil conj []) current))
            context
            (update context ::paths update-paths g current current-path)]
        [context (assoc acc current current-path)
         (sort-by (some-fn #(get-in context [::paths % ::dist])
                           (constantly Integer/MAX_VALUE))
                  queue)]))))

(defn nodes
  [g]
  (-> g
      (igraph/query '[:find ?node-id
                      :where (or [?node :to] [_ :target ?node])
                      [?node ::dsg/id ?node-id]])
      (->> (map :?node-id))))

(defn shortest-path
  [g node &
   {:keys [neighbor-fn weight-fn]
    :or   {neighbor-fn get-neighbors
           weight-fn   get-weight}}]
  (->> g
       nodes
       (cons node)
       (igraph/traverse g
                        (dijkstra-shortest-path-traversal neighbor-fn weight-fn)
                        {::paths {node {::dist 0}}}
                        {})))
