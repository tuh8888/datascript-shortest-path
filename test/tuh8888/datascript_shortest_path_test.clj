(ns tuh8888.datascript-shortest-path-test
  (:require
   [clojure.test :refer [deftest is]]
   [ont-app.datascript-graph.core :as dsg]
   [ont-app.igraph.core :as igraph]
   [tuh8888.datascript-shortest-path :as sut]))

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
    (is (= {:a {::sut/dist  0
                ::sut/nodes [:a]}
            :b {::sut/dist  1
                ::sut/nodes [:a :b]}
            :c {::sut/dist  1
                ::sut/nodes [:a :c]}
            :d {::sut/dist  1
                ::sut/nodes [:a :d]}
            :e {::sut/dist  3
                ::sut/nodes [:a :b :e]}}
           (-> g
               (sut/shortest-path :a)))))
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
    (is (= {::sut/dist  7
            ::sut/nodes [:a :c :d :g :b]}
           (-> g
               (sut/shortest-path :a)
               :b)))))
