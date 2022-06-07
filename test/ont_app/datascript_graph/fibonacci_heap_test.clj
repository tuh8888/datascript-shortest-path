(ns ont-app.datascript-graph.fibonacci-heap-test
  (:require
   [ont-app.datascript-graph.fibonacci-heap :as sut]
   [clojure.test :refer [deftest testing is]]))

(deftest insert-test
  (let [h (sut/make-heap)]
    (is (= 1
           (-> h
               (sut/insert 1)
               (sut/insert 2)
               sut/get-min)))
    (is (= 1
           (-> h
               (sut/insert 2)
               (sut/insert 1)
               sut/get-min))))
  (let [orig {:a 2
              :b 1}
        cmp  (fn [a b]
               (->> [a b]
                    (map orig)
                    (apply <)))
        h    (sut/make-heap cmp)]
    (is (= :b
           (-> h
               (sut/insert :a)
               (sut/insert :b)
               sut/get-min)))
    (is (= :b
           (-> h
               (sut/insert :b)
               (sut/insert :a)
               sut/get-min)))))

(deftest remove-min-test
  (let [h (-> (sut/make-heap)
              (sut/insert 1)
              (sut/insert 2)
              (sut/insert 3))]
    (is (= 1 (sut/get-min h)))
    (is (= 2
           (-> h
               sut/remove-min
               sut/get-min))))
  (let [h (-> (reduce sut/insert
                      (sut/make-heap)
                      (repeatedly 100 #(rand-int 100)))
              (sut/insert -1))]
    (is (= -1 (sut/get-min h)))
    (is (= -1
           (-> h
               (sut/insert -2)
               sut/remove-min
               sut/get-min)))))
