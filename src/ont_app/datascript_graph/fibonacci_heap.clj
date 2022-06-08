(ns ont-app.datascript-graph.fibonacci-heap
  (:require
   [w01fe.fibonacci-heap.core :as fh])
  (:import
   (clojure.lang ILookup IPersistentMap IPersistentStack IReduce MapEntry)
   (w01fe.fibonacci_heap FibonacciHeap)))

(defprotocol Heap
  (delete-min [h])
  (insert [h k])
  (get-min [h]))

(defrecord Node [parent child right left degree mark])

(defn make-node
  [k]
  {:left   k
   :right  k
   :mark   false
   :degree 0}
  #_(->Node nil nil k k 0 false))

(defn get-degree [h k] (get-in h [:data k :degree]))
(defn inc-degree [h k] (update-in h [:data k :degree] inc))
(defn dec-degree [h k] (update-in h [:data k :degree] dec))

(defn get-mark [h k] (get-in h [:data k :mark]))
(defn set-mark [h k x] (assoc-in h [:data k :mark] x))

(deftype Nav [ops])

(defn nav [& ops] (Nav. ops))

(defn get->
  [h op k]
  (let [k (if (= ::min k) (get-min h) k)]
    (cond (instance? Nav op) (reduce (fn [k op] (get-> h op k)) k (.ops op))
          :else              (get-in h [:data k op]))))

(defn set->
  [h loc k x]
  (let [should-nav (fn [k] (and (vector? k) (instance? Nav (first k))))
        resolve-k  (fn [k]
                     (let [k (if (= ::min k) (get-min h) k)]
                       (cond->> k (should-nav k) (apply get-> h))))
        k          (resolve-k k)
        x          (resolve-k x)]
    (assoc-in h [:data k loc] x)))

(comment
  (get-> {:data {:hi  {:left :bye}
                 :bye {:right 2}}}
         (nav :left :right)
         :hi))

(defn compare-keys
  [h a b]
  (cond (nil? a) false
        (nil? b) true
        :else    ((:cmp h) a b)))

(defn set-min [h k] (assoc h :min k))

(defn maybe-set-min
  [h k]
  (cond-> h
    (compare-keys h k (get-min h)) (assoc :min k)))

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
               A     []]
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
                    (let [[x y]  (if (compare-keys h x y) [y x] [x y])
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
         (remove nil?)
         (reduce (fn [h a] (maybe-set-min h a)) h))))

(defn cut
  [h k x min]
  (let [h (-> h
              (set-> :right [(nav :left) x] [(nav :right) x])
              (set-> :left [(nav :right) x] [(nav :left) x])
              (dec-degree k))
        h (cond (-> h
                    (get-degree k)
                    zero?)
                (set-> h :child k nil)
                (-> h
                    (get-> :child k)
                    (= x))
                (set-> h :child k [(nav :right) x])
                :else h)]
    (-> h
        (set-> :right x min)
        (set-> :left x [(nav :left) min])
        (set-> :left min x)
        (set-> :right [(nav :left) x] x)
        (set-> :parent x nil)
        (set-mark x false))))

(defn cascading-cut
  [h k min]
  (if-let [z (get-> h :parent k)]
    (if (get-mark h k)
      (-> h
          (cut z k min)
          (cascading-cut z min))
      (set-mark h k true))
    h))

(defn decrease-key
  [h k delete?]
  (let [orig-h h
        y      (get-> h :parent k)
        h      (cond-> h
                 (and (not (nil? y)) (or delete? (compare-keys h k y)))
                 (-> (cut y k (get-min h))
                     (cascading-cut y (get-min h))))
        h      (cond-> h
                 (or delete? (compare-keys h k (get-min h))) (set-min k))]
    #_(println (clojure.data/diff (:data orig-h) (:data h)))
    h))

(defn delete
  [h k]
  (let [orig-cmp (:cmp h)
        new-cmp  (fn [a b]
                   (cond (= k a) false
                         (= k b) true
                         :else   (orig-cmp a b)))]
    (-> h
        (assoc :cmp new-cmp)
        (decrease-key k true)
        delete-min
        (assoc :cmp orig-cmp))))


(defn remove-parents
  [h z]
  (loop [x (get-> h (nav :right :child) z)
         h h]
    (if (= x (get-> h :child z))
      h
      (recur (get-> :right h x) (set-> :parent h x nil)))))

(defrecord AFibonacciHeap [data min cmp]
  Heap
    (delete-min [h]
      (let [orig-h h]
        (if (nil? (get-min h))
          h
          (let [h (->
                    h
                    (cond->
                      (get-> h :child ::min)
                      ((fn [h]
                         (let [h            (-> h
                                                (set-> :parent
                                                       [(nav :child) ::min]
                                                       nil)
                                                (remove-parents ::min))
                               min-left     (get-> h :left ::min)
                               z-child-left (get-> h (nav :left :child) ::min)]
                           (-> h
                               (set-> :left ::min z-child-left)
                               (set-> :right z-child-left ::min)
                               (set-> :left [(nav :child) ::min] min-left)
                               (set-> :right min-left [(nav :child) ::min]))))))
                    (set-> :right [(nav :left) ::min] [(nav :right) ::min])
                    (set-> :left [(nav :right) ::min] [(nav :left) ::min]))
                z (get-min h)]
            (println (->> h
                          :data
                          #_(clojure.data/diff (:data orig-h))
                          #_(take 2)))
            (-> (if (= z (get-> h :right ::min))
                    (set-min h nil)
                    (-> h
                        (set-min (get-> h :right z))
                        consolidate))
                (update :data dissoc z))))))
    (insert [h k]
      (if (contains? (:data h) k)
        (decrease-key h k false)
        (let [h (assoc-in h [:data k] (make-node k))]
          (if (get-min h)
            (-> h
                (set-> :right k ::min)
                (set-> :left k [(nav :left) ::min])
                (set-> :left ::min k)
                (set-> :right [(nav :left) k] k)
                (maybe-set-min k))
            (set-min h k)))))
    (get-min [h] (:min h)))

(defn a-make-heap ([cmp] (->AFibonacciHeap {} nil cmp)) ([] (a-make-heap <)))

(comment
  (let [h (-> (a-make-heap)
              (insert 1)
              (insert 2)
              (insert 3)
              (insert 4))]
    (-> h
        (delete 2)
        get-min)))

(deftype MyFibonacciHeap [^FibonacciHeap h ^:unsynchronized-mutable node-map]
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (.size h))
    (assoc [this item priority]
      (if-let [node (node-map item)]
        (do (fh/decrease-key! h node priority item) this)
        (let [node (fh/add! h priority item)]
          (set! node-map (assoc node-map item node))
          this)))
    (empty [_] (MyFibonacciHeap. (fh/fibonacci-heap) {}))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ item] (contains? node-map item))
    (entryAt [_ k]
      (when-let [node (node-map k)]
        (fh/node->entry node)))
    (seq [_]
      (when-not (fh/empty? h)
        (->> h
             fh/peek-seq
             (map second)
             seq)))
    (without [this item]
      (fh/remove! h (node-map item))
      (set! node-map (dissoc node-map item))
      this)
  ILookup
    (valAt [_ item]
      (when-let [node (node-map item)]
        (fh/node-key node)))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_]
      (let [node (.min h)]
        (MapEntry. (.getData node) (.getKey node))))
    (pop [this] (fh/remove-min! h) this)
  IReduce
    (reduce [this f]
      (let [val (peek this)]
        (pop this)
        (reduce f val this)))
    (reduce [this f start]
      (loop [ret start]
        (if (empty? this)
          ret
          (let [val (peek this)]
            (pop this)
            (let [ret (f ret val)]
              (if (reduced? ret) @ret (recur ret))))))))

(defmethod print-method MyFibonacciHeap [o w] (print-method (seq o) w))

(defn make-heap [] (MyFibonacciHeap. (fh/fibonacci-heap) {}))

(comment
  (let [queue (-> (make-heap)
                  (assoc :a 1)
                  (assoc :b 2)
                  (assoc :c 3)
                  (assoc :d 0))]
    (println queue)
    (reduce (fn [v k] (when (= k :a) (assoc queue :c 1)) (conj v k))
            []
            queue)))
