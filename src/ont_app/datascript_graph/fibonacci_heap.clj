(ns ont-app.datascript-graph.fibonacci-heap
  (:require
   [ont-app.datascript-graph.util :refer [cond-pred->]]
   [clojure.zip :as zip]))

#_(defprotocol Heap
    (delete [h k])
    (insert [h k])
    (get-min [h]))

(defrecord Node [parent child right left degree mark])

(defrecord FibonacciHeap [data min cmp]
  #_Heap
  #_(delete [h k])
  #_(insert [h k])
  #_(get-min [h]))

(deftype Nav [ops])

(defn nav [& ops] (Nav. ops))
(nav :left :right)

(defn make-heap ([cmp] (->FibonacciHeap {} nil cmp)) ([] (make-heap <)))

(defn make-node [k] (->Node nil nil k k 0 false))

(defn get-min [h] (:min h))

(defn get->
  [h op k]
  (let [k (if (= ::min k) (get-min h) k)]
    (cond (instance? Nav op) (reduce (fn [k op] (get-> h op k)) k (.ops op))
          :else              (get-in h [:data k op]))))

(comment
  (get-> {:data {:hi  {:left :bye}
                 :bye {:right 2}}}
         (nav :left :right)
         :hi))

(defn get-degree [h k] (get-in h [:data k :degree]))
(defn get-mark [h k] (get-in h [:data k :mark]))


(defn compare-keys [h a b] ((:cmp h) a b))

(defn set->
  [h loc k x]
  (let [should-nav (fn [k] (and (vector? k) (instance? Nav (first k))))
        resolve-k  (fn [k]
                     (let [k (if (= ::min k) (get-min h) k)]
                       (cond->> k (should-nav k) (apply get-> h))))
        k          (resolve-k k)
        x          (resolve-k x)]
    (assoc-in h [:data k loc] x)))


(defn set-mark [h k x] (assoc-in h [:data k :mark] x))

(defn set-min [h k] (assoc h :min k))

(defn inc-degree [h k] (update-in h [:data k :degree] inc))
(defn dec-degree [h k] (update-in h [:data k :degree] dec))

(defn cut
  [h node x]
  (let [xl [(nav :left) x]
        xr [(nav :right) x]
        h  (-> h
               (set-> :right [(nav :left) x] [(nav :right) x])
               (set-> :left [(nav :right) x] [(nav :left) x])
               (dec-degree node))
        h  (cond (-> h
                     (get-degree node)
                     zero?)
                 (set-> :child h node nil)
                 (-> h
                     (get-> :child node)
                     (= x))
                 (set-> :child h node [(nav :right) x]))]
    (-> h
        (set-> :right x ::min)
        (set-> :left x [(nav :left) ::min])
        (set-> :left ::min x)
        (set-> :right [(nav :left) x] x)
        (set-> :parent x nil)
        (set-mark x false))))

(defn cascading-cut
  [h node]
  (let [z (get-> h :parent node)]
    (cond-> h
      z ((fn [h]
           (if (get-mark h node)
             (-> h
                 (cut z node)
                 (cascading-cut z))
             (set-mark h node true)))))))

(defn insert
  [h node]
  (let [h (assoc-in h [:data node] (make-node node))]
    (if (get-min h)
      (-> h
          (set-> :right node ::min)
          (set-> :left node [(nav :left) ::min])
          (set-> :left ::min node)
          (set-> :right [(nav :left) node] node)
          (cond-> (compare-keys h node (get-min h)) (set-min node)))
      (set-min h node))))

(defn link
  [h node new-parent]
  (let [h (-> h
              (set-> :right [(nav :left) node] [(nav :right) node])
              (set-> :left [(nav :right) node] [(nav :left) node])
              (set-> :parent node new-parent))
        h (if (get-> :child h new-parent)
            (-> h
                (set-> :left node [(nav :child) new-parent])
                (set-> :right node [(nav :child :right) new-parent])
                (set-> :right [(nav :child) new-parent] node)
                (set-> :left [(nav :right) node] node))
            (-> h
                (set-> :child new-parent node)
                (set-> :right node node)
                (set-> :left node node)))]
    (-> h
        (inc-degree new-parent)
        (set-mark new-parent false))))


(defn consolidate
  [h]
  (let [[start h A]
        (loop [start (get-min h)
               w     start
               h     h
               A     {}]
          (let [x w
                next-w (get-> h :right w)
                d (get-degree h x)
                [d x start next-w h A]
                (loop [d      d
                       x      x
                       start  start
                       next-w next-w
                       h      h
                       A      A]
                  (if-let [y (get A d)]
                    (let [[x y]  (cond-> [x y]
                                   (compare-keys h y x) reverse)
                          start  (cond->> start (= y start) (get-> h :right))
                          next-w (cond->> next-w (= y next-w) (get-> h :right))
                          h      (link h y x)]
                      (recur (inc d) x start next-w h (assoc A d nil)))
                    [d x start next-w h A]))
                A (assoc A d x)
                w next-w]
            (if (= w start) [start h A] (recur w start h A))))
        h (set-min h start)]
    (->> A
         vals
         (remove nil?)
         (reduce (fn [h a]
                   (cond-> h
                     (compare-keys h a (get-min h)) (set-min a)))
                 h))))

(defn remove-min
  [h]
  (let [z (get-min h)]
    (if (nil? z)
      h
      (let [h (cond-> h
                (get-> h :child z)
                ((fn [h]
                   (let [h            (set-> h :parent [(nav :child) z] nil)
                         h            (loop [x (get-> h (nav :right :child) z)
                                             h h]
                                        (if (= x (get-> h :child z))
                                          h
                                          (recur (get-> :right h x)
                                                 (set-> :parent h x nil))))
                         min-left     (get-> :left h z)
                         z-child-left (get-> (nav :left :child) h z)]
                     (-> h
                         (set-> :left z z-child-left)
                         (set-> :right z-child-left z)
                         (set-> :left [(nav :child) z] min-left)
                         (set-> :right min-left [(nav :child) z]))))))
            h (-> h
                  (set-> :right [(nav :left) z] [(nav :right) z])
                  (set-> :left [(nav :right) z] [(nav :left) z]))]
        (if (= z (get-> h :right z))
          (set-min h nil)
          (-> h
              (set-min (get-> h :right z))
              consolidate
              (update :data dissoc z)))))))

(let [orig {:a 3
            :b 2
            :c 4}]
  (-> (make-heap (fn [a b] (< (orig a) (orig b))))
      (insert :a)
      (insert :b)
      (insert :c)
      remove-min
      ((juxt get-min (comp keys :data)))))
