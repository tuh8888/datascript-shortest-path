(ns ont-app.datascript-graph.fibonacci-heap
  (:require
   [w01fe.fibonacci-heap.core :as fh])
  (:import
   (clojure.lang ILookup IPersistentMap IPersistentStack IReduce MapEntry Atom)
   (w01fe.fibonacci_heap FibonacciHeap)))

(defrecord ANode [parent child right left degree mark])

(defn make-node
  [k x]
  (let [node (atom (map->ANode {:key    k
                                :data   x
                                :mark   false
                                :degree 0}))]
    (swap! node assoc :right node)
    (swap! node assoc :left node)
    node))


(defn get-degree [h k] (get-in h [:nodes k :degree]))
(defn inc-degree [h k] (update-in h [:nodes k :degree] inc))
(defn dec-degree [h k] (update-in h [:nodes k :degree] dec))

(defn get-mark [h k] (get-in h [:nodes k :mark]))
(defn set-mark [h k x] (assoc-in h [:nodes k :mark] x))

(deftype Nav [ops])

(defn nav [& ops] (Nav. ops))

(defn get->
  [h op k]
  (let [k (if (= ::min k) (get-min h) k)]
    (cond (instance? Nav op) (reduce (fn [k op] (get-> h op k)) k (.ops op))
          :else              (get-in h [:nodes k op]))))

(defn set->
  [h loc k x]
  (let [should-nav (fn [k] (and (vector? k) (instance? Nav (first k))))
        resolve-k  (fn [k]
                     (let [k (if (= ::min k) (get-min h) k)]
                       (cond->> k (should-nav k) (apply get-> h))))
        k          (resolve-k k)
        x          (resolve-k x)]
    (assoc-in h [:nodes k loc] x)))

(comment
  (get-> {:nodes {:hi  {:left :bye}
                  :bye {:right 2}}}
         (nav :left :right)
         :hi))

(defn compare-keys
  [h a b]
  (cond (nil? a) false
        (nil? b) true
        :else    ((:cmp h) a b)))

(defrecord AFibonacciHeap [min cmp])

(defn cut
  [this x min]
  (swap! (:left @x) assoc :right (:right @x))
  (swap! (:left @x) assoc :left (:left @x))
  (swap! this update :degree dec)
  (if (= (:degree @this) 0)
    (swap! this assoc :child nil)
    (when (= (:child this) x) (swap! this assoc :child (:right @x))))
  (swap! x assoc :right min)
  (swap! x assoc :left (:left @min))
  (swap! min assoc :left x)
  (swap! (:left @x) assoc :right x)
  (swap! x assoc :parent nil)
  (swap! x assoc :mark false))

(defn cascading-cut
  [this min]
  (let [z (:parent @this)]
    (when (not (nil? z))
      (if (:mark @this)
        (do (cut z this min) (cascading-cut z min))
        (swap! this assoc :mark true)))))


(defn link
  [this parent]
  (swap! (:left @this) assoc :right (:right @this))
  (swap! (:right @this) assoc :left (:left @this))
  (swap! this assoc :parent parent)
  (if (= (:child @parent) nil)
    (do (swap! parent assoc :child this)
        (swap! this assoc :right this)
        (swap! this assoc :left this))
    (do (swap! this assoc :left (:child @parent))
        (swap! this assoc :right (:right @(:child @parent)))
        (swap! (:child @parent) assoc :right this)
        (swap! (:right @this) assoc :left this)))
  (swap! parent update :degree inc)
  (swap! this assoc :mark false))

(defn add-to-list
  [this l]
  (loop [cur this
         l   l]
    (let [l   (conj l cur)
          l   (if (not (nil? (:child cur))) (add-to-list @(:child cur) l) l)
          cur @(:right cur)]
      (if (= this cur) l (recur cur l)))))

(defn consolidate
  [this]
  (println "here2" (:data @(:min @this)))
  (let [A     (make-array Atom 45)
        start (atom (:min @this))
        w     (atom (:min @this))]
    (loop []
      (let [x      (atom @w)
            next-w (atom (:right @@w))
            d      (atom (:degree @@x))]
        (println "here5" (:data @(:min @this)))
        (while (not (nil? (aget A @d)))
               (println "here7" (:data @(:min @this)))
               (let [y (atom (aget A @d))]
                 (when (> (compare (:key @@x) (:key @@y)) 0)
                   (let [temp @y]
                     (reset! y @x)
                     (reset! x temp)))
                 (println "here8" (:data @(:min @this)))
                 (when (= @y @start) (reset! start (:right @@start)))
                 (when (= @y @next-w) (reset! next-w (:right @@next-w)))
                 (println "here9" (:data @(:min @this)))
                 (println (:data @@y) (:data @@x))
                 (link @y @x)
                 (println "here10" (:data @(:min @this)))
                 (aset A @d nil)
                 (swap! d inc)))
        (println "here6" (:data @(:min @this)))
        (aset A @d @x)
        (reset! w @next-w)
        (println "here4" (:data @(:min @this))))
      (when (not= @w @start) (recur)))
    (println "here3" (:data @(:min @this)))
    (swap! this assoc :min @start)
    (doseq [a A]
      (when (and (not (nil? a)) (< (compare (:key a) (:key (:min @this))) 0))
        (swap! this assoc :min a)))))

(comment
  (let [queue        (-> (a-make-heap))
        (assoc :a 1) (assoc :b 2)
        (assoc :c 3) (assoc :d 0)]
    (println queue)
    (reduce (fn [v k] (when (= k :a) (assoc queue :c 1)) (conj v k))
            []
            queue)))

(comment
  (let [h     (a-make-heap)
        nodes (->> {:a 1
                    :b 2
                    :c 3
                    :d 0
                    #_#_:d 4}
                   (reduce (fn [m [x k]]
                             (assoc m x (insert h x k)))
                           {}))]
    (update-vals nodes (comp :key deref))
    #_(doall (map (comp :data deref) nodes))
    #_(decrease-key h)
    #_(reduce (fn [v _] (conj v (remove-min h))) [] (range 3))
    #_(:data @(:min @h))
    #_[@h nodes]
    #_(-> h
          (delete 2)
          get-min)))
(defn remove-min
  [this]
  (let [z (:min @this)]
    (if (nil? z)
      this
      (do (when (:child @z)
            (swap! (:child @z) assoc :parent nil)
            (let [x (atom (:right @(:child @z)))]
              (while (not= @x (:child @z))
                     (swap! @x assoc :parent nil)
                     (reset! x (:right @@x))))
            (let [min-left     (:left @(:min @this))
                  z-child-left (:left @(:child @z))]
              (swap! (:min @this) assoc :left z-child-left)
              (swap! z-child-left assoc :right (:min @this))
              (swap! (:child @z) assoc :left min-left)
              (swap! min-left assoc :right (:child @z))))
          (swap! (:left @z) assoc :right (:right @z))
          (swap! (:right @z) assoc :left (:left @z))
          (if (= z (:right @z))
            (do (println "here") (swap! this assoc :min nil))
            (do (swap! this assoc :min (:right @z)) (consolidate this)))
          #_(set! n (dec n))
          (:data @z)))))


(defn insert
  [this x k]
  (let [node (make-node k x)]
    (if (:min @this)
      (do (swap! node assoc :right (:min @this))
          (swap! node assoc :left (:left @(:min @this)))
          (swap! (:min @this) assoc :left node)
          (swap! (:left @node) assoc :right node)
          (when (< (compare k (:key @(:min @this))) 0)
            (swap! this assoc :min node)))
      (swap! this assoc :min node))
    node))

(defn get-min [this] (:min @this))

(defn a-make-heap
  ([cmp] (atom (->AFibonacciHeap nil cmp)))
  ([] (a-make-heap <)))

(defmethod print-method ANode
  [o w]
  (print-method (:key o) w)
  #_(let [remove-self (fn [n] (if (instance? Atom n) (:key @n) nil))]
      (-> o
          (update :right remove-self)
          (update :left remove-self)
          (update :child remove-self)
          (update :parent remove-self)
          (->> (into {}))
          (print-method w))))


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
