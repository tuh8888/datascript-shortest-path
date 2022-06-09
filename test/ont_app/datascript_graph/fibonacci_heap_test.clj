(ns ont-app.datascript-graph.fibonacci-heap-test
  (:require
   [ont-app.datascript-graph.fibonacci-heap :as sut]
   [clojure.test :refer [deftest is]]))

(deftest insert-test
  (let [h (sut/make-heap)]
    (is (= 1
           (-> h
               (assoc 1 1)
               (assoc 2 2)
               peek
               key)))
    (is (= 1
           (-> h
               (assoc 2 2)
               (assoc 1 1)
               peek
               key)))
    (is (= 1
           (-> h
               (assoc 2 2)
               (assoc 1 1)
               pop
               (assoc 1 1)
               peek
               key)))
    (is (= 2
           (-> h
               (assoc 2 2)
               (assoc 1 1)
               (assoc 1 1)
               pop
               peek
               key))))
  (let [orig {:a 2
              :b 1}
        cmp  (fn [a b]
               (->> [a b]
                    (map orig)
                    (apply <)))
        h    (sut/make-heap cmp)]
    (is (= :b
           (-> h
               (assoc :a :a)
               (assoc :b :b)
               peek
               key)))
    (is (= :b
           (-> h
               (assoc :b :b)
               (assoc :a :a)
               peek
               key)))))

(deftest delete-min-test
  (let [h (-> (sut/make-heap)
              (assoc 1 1)
              (assoc 2 2)
              (assoc 3 3))]
    (is (= 1 (key (peek h))))
    (is (= 2
           (-> h
               pop
               peek
               key))))
  (let [h (-> (reduce (fn [h v] (assoc h v v))
                      (sut/make-heap)
                      (repeatedly 100 #(rand-int 100)))
              (assoc -1 -1))]
    (is (= -1 (key (peek h))))
    (is (= -1
           (-> h
               (assoc -2 -2)
               pop
               peek
               first)))))

(deftest delete
  (let [h (-> (sut/make-heap)
              (assoc 1 1)
              (assoc 2 2)
              (assoc 3 3)
              (assoc 4 4))]
    (is (= 1 (key (peek h))))
    (is (= 1
           (-> h
               (dissoc 2)
               peek
               key)))
    (is (= 3
           (-> h
               (dissoc 2)
               (dissoc 1)
               peek
               key)))
    (is (= 3
           (-> h
               (dissoc 1)
               (dissoc 2)
               peek
               key)))))
