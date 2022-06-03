(ns tuh8888.datascript-shortest-path-test
  (:require
   [clojure.test        :refer [deftest is]]
   [datascript.core     :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.igraph.core :as igraph]
   [tuh8888.datascript-shortest-path :as sut]))

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
       (d/pull-many (:db g) [{::sut/to [::dsg/id]} :weight])
       (map #(update % ::sut/to (comp ::dsg/id first)))))

(defn path-info
  [g path]
  (-> {}
      (assoc :dist  (calc-edge-dist g path)
             :nodes (->> path
                         (edges->node-path g)
                         (map ::sut/to)
                         (cons :a)
                         vec))))

(defn get-min-successors
  [g node]
  (let [min-weight (fn [db a b]
                     (d/q '{:find  [(min ?dist) .]
                            :in    [?ma ?mb $ %]
                            :where [(edge ?ma _ ?mb ?medge)
                                    [?medge :weight ?dist]]}
                          a
                          b
                          db
                          sut/graph-rules))]
    (d/q '{:find  [?b-id ?edge]
           :in    [$ ?a min-weight %]
           :where [(edge ?a _ ?b ?edge)
                   [(min-weight $ ?a ?b) ?mdist]
                   [?edge :weight ?mdist]
                   [?b ::dsg/id ?b-id]]}
         (:db g)
         [::dsg/id node]
         min-weight
         sut/graph-rules)))

(deftest shortest-path-test
  (let [g (-> {:weight {:db/type :db.type/integer}}
              (dsg/make-graph)
              (igraph/add (sut/complex-triples [[:a
                                                 :to
                                                 {:b {:weight 2}}
                                                 :to
                                                 {:b {:weight 1}
                                                  :c {:weight 1}
                                                  :d {:weight 1}}]
                                                [:b
                                                 :to
                                                 {:e {:weight 2}}]])))]
    (is (= {:a {:dist  0
                :nodes [:a]}
            :b {:dist  1
                :nodes [:a :b]}
            :c {:dist  1
                :nodes [:a :c]}
            :d {:dist  1
                :nodes [:a :d]}
            :e {:dist  3
                :nodes [:a :b :e]}}
           (-> g
               (sut/shortest-path :a get-min-successors calc-edge-dist)
               (update-vals (partial path-info g))))))
  ;; From https://brilliant.org/wiki/dijkstras-short-path-finder/#examples
  (let [g (-> {:weight {:db/type :db.type/integer}}
              (dsg/make-graph)
              (igraph/add (sut/complex-triples [[:a
                                                 :to
                                                 {:c {:weight 3}
                                                  :d {:weight 7}
                                                  :e {:weight 5}}]
                                                [:c
                                                 :to
                                                 {:a {:weight 3}
                                                  :f {:weight 7}
                                                  :d {:weight 1}}]
                                                [:d
                                                 :to
                                                 {:a {:weight 7}
                                                  :c {:weight 1}
                                                  :f {:weight 2}
                                                  :g {:weight 1}
                                                  :h {:weight 3}
                                                  :e {:weight 3}}]
                                                [:e
                                                 :to
                                                 {:a {:weight 5}
                                                  :d {:weight 3}
                                                  :h {:weight 2}}]
                                                [:f
                                                 :to
                                                 {:c {:weight 7}
                                                  :d {:weight 2}
                                                  :g {:weight 2}
                                                  :i {:weight 1}}]
                                                [:g
                                                 :to
                                                 {:d {:weight 1}
                                                  :f {:weight 2}
                                                  :h {:weight 3}
                                                  :i {:weight 3}
                                                  :b {:weight 2}}]
                                                [:h
                                                 :to
                                                 {:d {:weight 3}
                                                  :e {:weight 2}
                                                  :g {:weight 3}
                                                  :b {:weight 4}}]
                                                [:i
                                                 :to
                                                 {:f {:weight 1}
                                                  :g {:weight 3}
                                                  :b {:weight 5}}]])))]
    (is (= {:dist  7
            :nodes [:a :c :d :g :b]}
           (-> g
               (sut/shortest-path :a get-min-successors calc-edge-dist)
               :b
               (->> (path-info g)))))))
