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

(deftest decrease-priority-test
  (let [h (sut/make-heap)]
    (is (= 2
           (-> h
               (assoc 1 1)
               (assoc 2 2)
               (assoc 2 0)
               peek
               key)))
    (is (= 2
           (-> h
               (assoc 1 1)
               pop
               (assoc 2 2)
               (assoc 1 1)
               (assoc 2 0)
               peek
               key)))
    (is (= 2
           (-> h
               (assoc 2 2)
               (assoc 1 1)
               pop
               (assoc 1 1)
               (assoc 2 0)
               peek
               key)))
    (is (= 2
           (-> h
               (assoc 2 2)
               (assoc 1 1)
               pop
               (assoc 1 1)
               (assoc 2 0)
               (assoc 1 -1)
               pop
               (assoc 1 -1)
               (assoc 2 -2)
               peek
               key)))))


(deftest heap-test
  (binding [sut/debug false]
    (let [n1 "foo"
          n2 "bar"
          n3 "baz"
          h  (-> (sut/make-heap)
                 (assoc n1 50)
                 (assoc n2 50)
                 (assoc n3 75))]
      (is (seq h))
      (is (= (sut/node-get n1 (.nodes h) :priority) 50))
      (is (= 3 (count h)))
      (is (= ["foo" "bar" "baz"] (seq h)))
      (let [h (dissoc h n2)]
        (is (= (count h) 2))
        (is (= ["foo" "baz"] (seq h)))
        (doseq [rep (range 10)]
          (let [n-elems 1000
                #_10000
                #_#_c
                  [35 77 52 70 5 16 97 47 65 45 36 90 71 92 30 22 25 58 3 4 64
                   42 37 83 23 13 85 84 28 62 87 20 74 78 88 73 46 79 53 24 81
                   72 38 68 9 50 44 12 7 6 40 1 54 14 43 91 19 33 32 93 17 69 11
                   34 56 61 80 63 66 39 48 2 10 60 94 95 49 15 29 76 75 41 98 8
                   89 31 86 26 18 55 59 0 51 27 57 21 96 82 67 99]
                c       (->> n-elems
                             range
                             shuffle
                             #_((fn [c] (println "shuffled" c) c)))
                h       (reduce (fn [h v] (assoc h v v)) h c)]
            (is (= (+ 2 n-elems) (count h)))
            (let [removed (min 50 n-elems)
                  h       (->> removed
                               range
                               (reduce (fn [h i]
                                         (is (= [i i] (peek h)))
                                         (is (= (- (+ 2 n-elems) i) (count h)))
                                         (pop h))
                                       h))
                  h       (assoc h n3 1)]
              (is (= (- (+ n-elems 2) removed) (count h)))
              (is (= (peek h) [n3 1]))
              (let [h       (-> h
                                (dissoc n1)
                                (dissoc n3))
                    removed (- n-elems removed)
                    h       (->> removed
                                 range
                                 (reduce (fn [h i]
                                           (let [i (+ 50 i)]
                                             (is (= [i i] (peek h)))
                                             (pop h)))
                                         h))]
                (is (= 0 (count h)))
                (is (empty? h))))))))))
