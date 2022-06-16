(ns ont-app.datascript-graph.path-test
  (:require
   [clojure.test         :refer [deftest is testing]]
   [datascript.core      :as d]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.datascript-graph.path :as sut]
   [mops.datascript-mops :as dm]))

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
       (d/pull-many (:db g) [{::dm/to [::dsg/id]} :weight])
       (map #(update % ::dm/to (comp ::dsg/id first)))))

(defn path-info
  [g source path]
  (-> {}
      (cond-> (not (string? path)) (assoc :dist  (calc-edge-dist g path)
                                          :nodes (->> path
                                                      (edges->node-path g)
                                                      (map ::dm/to)
                                                      (cons source)
                                                      vec)))))

(defn min-weight-successors
  [g node]
  (let [min-weight (fn [db a b]
                     (d/q '{:find  [(min ?dist) .]
                            :in    [?ma ?mb $ %]
                            :where [(edge ?ma _ ?mb ?medge)
                                    [?medge :weight ?dist]]}
                          a
                          b
                          db
                          dm/graph-rules))]
    (dm/query g
              '{:find  [?from-id ?to-id ?edge ?id]
                :keys  [from to e id]
                :in    [?from min-weight]
                :where [(edge ?from _ ?to ?edge)
                        [(min-weight $ ?from ?to) ?mdist]
                        [?edge :weight ?mdist]
                        [?edge ::dsg/id ?id]
                        [?from ::dsg/id ?from-id]
                        [?to ::dsg/id ?to-id]]}
              [::dsg/id node]
              min-weight)))

(deftest dijkstra-shortest-path-test
  (let [g (-> {:weight {:db/type :db.type/integer}}
              dm/make-mop-graph
              (dm/add-mop [:a
                           :to
                           {:b {:weight 2}}
                           :to
                           {:b {:weight 1}
                            :c {:weight 1}
                            :d {:weight 1}}])
              (dm/add-mop [:b
                           :to
                           {:e {:weight 2}}]))]
    (let [source :a]
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
                 (sut/shortest-path source min-weight-successors calc-edge-dist)
                 (update-vals (partial path-info g source))))))
    (let [source :e]
      (is (= {:e {:dist  0
                  :nodes [:e]}}
             (-> g
                 (sut/shortest-path source min-weight-successors calc-edge-dist)
                 (update-vals (partial path-info g source)))))))
  ;; From https://brilliant.org/wiki/dijkstras-short-path-finder/#examples
  (let [g (-> (reduce dm/add-mop
                      (dm/make-mop-graph {:weight {:db/type :db.type/integer}})
                      [[:a
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
                         :b {:weight 5}}]]))
        source :a
        target :b]
    (is (= {:dist  7
            :nodes [:a :c :d :g :b]}
           (-> g
               (sut/shortest-path source min-weight-successors calc-edge-dist)
               target
               (->> (path-info g source)))))))

(deftest bellman-ford-shortest-path-test
  (let [g (-> {:weight {:db/type :db.type/integer}}
              dm/make-mop-graph
              (dm/add-mop [:a
                           :to
                           {:b {:weight 2}}
                           :to
                           {:b {:weight 1}
                            :c {:weight 1}
                            :d {:weight 1}}])
              (dm/add-mop [:b
                           :to
                           {:e {:weight 2}}]))]
    (let [source :a]
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
                 (sut/shortest-path source
                                    min-weight-successors
                                    calc-edge-dist
                                    :alg
                                    ::sut/bellman-ford)
                 (update-vals (partial path-info g source))))))
    (let [source :e]
      (is (= {:e {:dist  0
                  :nodes [:e]}}
             (-> g
                 (sut/shortest-path source
                                    min-weight-successors
                                    calc-edge-dist
                                    :alg
                                    ::sut/bellman-ford)
                 (update-vals (partial path-info g source)))))))
  (testing "Detect negative cycles"
   (let [g (-> {:weight {:db/type :db.type/integer}}
               dm/make-mop-graph
               (dm/add-mop [:a
                            :to
                            {:b {:weight 2}}
                            :to
                            {:b {:weight 1}
                             :c {:weight 1}
                             :d {:weight 1}}])
               (dm/add-mop [:b
                            :to
                            {:e {:weight 2}
                             :c {:weight -2}}])
               (dm/add-mop [:c
                            :to
                            {:a {:weight -2}}]))]
     (is (= "Negative weight cycle"
            (try (sut/shortest-path g
                                    :a
                                    min-weight-successors
                                    calc-edge-dist
                                    :detect-neg?
                                    true)
                 (catch clojure.lang.ExceptionInfo e (ex-message e)))))))
  ;; From https://brilliant.org/wiki/dijkstras-short-path-finder/#examples
  (let [g      (reduce dm/add-mop
                       (dm/make-mop-graph {:weight {:db/type :db.type/integer}})
                       [[:a
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
                          :b {:weight 5}}]])
        source :a
        target :b]
    (is (= {:dist  7
            :nodes [:a :c :d :g :b]}
           (-> g
               (sut/shortest-path source
                                  min-weight-successors
                                  calc-edge-dist
                                  :alg
                                  ::sut/bellman-ford)
               target
               (->> (path-info g source)))))))

(deftest johnson-all-pairs-shortest-path
  (let [g (-> {:weight {:db/type :db.type/integer}}
              dm/make-mop-graph
              (dm/add-mop [:a
                           :to
                           {:b {:weight 2}}
                           :to
                           {:b {:weight 1}
                            :c {:weight 1}
                            :d {:weight 1}}])
              (dm/add-mop [:b
                           :to
                           {:e {:weight 2}}]))]
    (is (= {:a {:a {:dist  0
                    :nodes [:a]}
                :b {:dist  1
                    :nodes [:a :b]}
                :c {:dist  1
                    :nodes [:a :c]}
                :d {:dist  1
                    :nodes [:a :d]}
                :e {:dist  3
                    :nodes [:a :b :e]}}
            :b {:b {:dist  0
                    :nodes [:b]}
                :e {:dist  2
                    :nodes [:b :e]}}
            :c {:c {:dist  0
                    :nodes [:c]}}
            :d {:d {:dist  0
                    :nodes [:d]}}
            :e {:e {:dist  0
                    :nodes [:e]}}}
           (-> g
               (sut/johnson-all-pairs-shortest-paths min-weight-successors
                                                     calc-edge-dist
                                                     :weight)
               (->> (map (juxt key
                               #(update-vals (val %)
                                             (partial path-info g (key %)))))
                    (into {}))))))
  (testing "Detect negative cycles"
   (let [g (-> {:weight {:db/type :db.type/integer}}
               dm/make-mop-graph
               (dm/add-mop [:a
                            :to
                            {:b {:weight 2}}
                            :to
                            {:b {:weight 1}
                             :c {:weight 1}
                             :d {:weight 1}}])
               (dm/add-mop [:b
                            :to
                            {:e {:weight 2}
                             :c {:weight -2}}])
               (dm/add-mop [:c
                            :to
                            {:a {:weight -2}}]))]
     (is (= "Negative weight cycle"
            (try (sut/johnson-all-pairs-shortest-paths g
                                                       min-weight-successors
                                                       calc-edge-dist
                                                       :weight)
                 (catch clojure.lang.ExceptionInfo e (ex-message e)))))))
  ;; From https://brilliant.org/wiki/dijkstras-short-path-finder/#examples
  (let [g      (reduce dm/add-mop
                       (dm/make-mop-graph {:weight {:db/type :db.type/integer}})
                       [[:a
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
                          :b {:weight 5}}]])
        source :a
        target :b]
    (is (= {:dist  7
            :nodes [:a :c :d :g :b]}
           (-> g
               (sut/johnson-all-pairs-shortest-paths min-weight-successors
                                                     calc-edge-dist
                                                     :weight)
               source
               target
               (->> (path-info g source)))))))
