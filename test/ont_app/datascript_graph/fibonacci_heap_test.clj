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
    #_(is (= 3
             (-> h
                 (dissoc 2)
                 (dissoc 1)
                 peek
                 key)))
    #_(is (= 3
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
  (let [h  (-> (sut/make-heap)
               (assoc "foo" 50)
               (assoc "bar" 50)
               (assoc "baz" 75))
        n1 "foo"
        n2 "bar"
        n3 "baz"]
    (is (not (empty? h)))
    (is (= (sut/node-get n1 (.m h) :priority) 50))
    #_(is (= (node-val n1) "foo"))
    #_(is (= (node->entry n1) [50 "foo"]))
    (is (= 3 (count h)))
    (is (= ["foo" "bar" "baz"] (seq h)))
    (let [h (dissoc h n2)]
      (is (= (count h) 2))
      (is (= ["foo" "baz"] (seq h)))
      (let [n-elems 5
            c       [1 0 4 3 2]
            #_c
            #_(->> c
                   n-elems
                   range
                   shuffle
                   ((fn [c] (println "shuffled" c) c)))
            h       (reduce (fn [h v] (assoc h v v)) h c)
            #_(doseq [i (shuffle (range 100))]
                (add! h [i] i))]
        (is (= (count h) (+ 2 n-elems)))
        (let [removed 1
              h       (-> [h i]
                          (fn (println i) (is (= (peek h) [i i])) (pop h))
                          (reduce h (range removed)))
              #_#_h (assoc h n3 1)]
          #_(decrease-key! h n3 [1] "boo")
          #_(is (= (- (+ n-elems 2) removed) (count h)))
          #_(is (= (peek h) [n3 #_"boo" 1]))
          #_(let [h (-> h
                        (dissoc h n1)
                        (dissoc h n3))
                  h (reduce (fn [h i]
                              (let [i (+ 50 i)]
                                (is (= [i i] (peek h)))
                                (pop h)))
                            h
                            (range 50))]
              #_(dotimes [i 50]
                  (let [i (+ 50 i)]
                    (is (= (peek-min h) [[i] i]))
                    (is (= (remove-min! h) [[i] i]))))
              (is (= (count h) 0))
              (is (empty? h))))))))
