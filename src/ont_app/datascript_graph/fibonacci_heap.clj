(ns ont-app.datascript-graph.fibonacci-heap
  (:import
   (clojure.lang ILookup IPersistentMap IPersistentStack IReduce MapEntry)))

(defrecord Node [priority parent child right left degree mark])

(defn make-node
  [k priority]
  (map->Node {:priority priority
              :mark     false
              :degree   0
              :left     k
              :right    k}))

(defn node-get [k m prop] (get-in m [k prop]))

(defn compare-priorities
  [min-node m cmp n1 n2]
  (let [p1 (node-get n1 m :priority)
        p2 (node-get n2 m :priority)]
    (if (some nil? [p1 p2])
      (throw (ex-info ""
                      {:n1  n1
                       :n2  n2
                       :m   m
                       :min min-node}))
      (cmp p1 p2))))

(defn check-node
  [m k]
  (let [vs {:child  :parent
            :parent :child
            :left   :right
            :right  :left}]
    (doseq [[v _] vs]
      (let [rev-v     (vs v)
            rev-k     (node-get k m v)
            rev-rev-k (node-get rev-k m rev-v)]
        (if (#{:parent} v)
          (let [c (loop [c #{rev-rev-k}
                         x rev-rev-k]
                    (let [next-x (node-get x m :right)]
                      (if (= next-x rev-rev-k)
                        c
                        (recur (conj c next-x) next-x))))]
            (when-not (and (nil? rev-rev-k) (#{:child :parent} v))
              (assert (c k)
                      {:k         k
                       :v         v
                       :rev-v     rev-v
                       :rev-k     rev-k
                       :rev-rev-k c
                       :m         (-> {}
                                      (into m)
                                      (update-vals #(into {} %)))})))
          (when-not (and (nil? rev-rev-k) (#{:child :parent} v))
            (assert (= k rev-rev-k)
                    {:k         k
                     :v         v
                     :rev-v     rev-v
                     :rev-k     rev-k
                     :rev-rev-k rev-rev-k})))))))

(def ^:dynamic debug false)

(defn check-nodes
  [h]
  (if debug
    (if (instance? FibonacciHeap h)
      (let [m        (.m h)
            min-node (.min-node h)]
        (assert (or (and (nil? min-node) (empty? m)) (contains? m min-node))
                {:min min-node
                 :m   (into {} m)})
        (doseq [[k _] m]
          (check-node m k))
        h)
      (let [m h]
        (doseq [[k _] h]
          (check-node h k))
        m))
    h))

(defn cut
  [m node x min-node]
  (let [m (-> m
              (check-nodes)
              (assoc-in [(node-get x m :left) :right] (node-get x m :right))
              (assoc-in [(node-get x m :right) :left] (node-get x m :left))
              (update-in [node :degree] dec))
        m (if (zero? (node-get node m :degree))
            (assoc-in m [node :child] nil)
            (cond-> m
              (= (node-get node m :child) x) (assoc-in [node :child]
                                              (node-get x m :right))))]
    (-> m
        (assoc-in [x :right] min-node)
        (assoc-in [x :left] (node-get min-node m :left))
        (assoc-in [min-node :left] x)
        ((fn [m] (assoc-in m [(node-get x m :left) :right] x)))
        (assoc-in [x :parent] nil)
        (check-nodes)
        (assoc-in [x :mark] false))))

(defn cascading-cut
  [m node min-node]
  (let [z (node-get node m :parent)]
    (if (nil? z)
      m
      (if (node-get node m :mark)
        (-> m
            (cut z node min-node)
            (cascading-cut z min-node))
        (assoc-in m [node :mark] true)))))

(defn connect-nodes
  [m left right]
  (-> m
      (assoc-in [right :left] left)
      (assoc-in [left :right] right)))

(defn link
  [m k parent]
  (let [m (-> m
              (check-nodes)
              (connect-nodes (node-get k m :left) (node-get k m :right))
              (assoc-in [k :parent] parent))
        m (if (node-get parent m :child)
            (-> m
                (connect-nodes (node-get parent m :child) k)
                (assoc-in [k :right]
                          (-> parent
                              (node-get m :child)
                              (node-get m :right)))
                ((fn [m] (assoc-in m [(node-get k m :right) :left] k)))
                (check-nodes))
            (-> m
                (assoc-in [parent :child] k)
                (connect-nodes k k)
                (check-nodes)))]
    (-> m
        (update-in [parent :degree] inc)
        (assoc-in [k :mark] false))))

(defprotocol Heap
  #_(consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(declare remove-min)

(defn consolidate
  [h]
  (let [m        (.m h)
        cmp      (.cmp h)
        min-node (.min-node h)]
    (check-nodes m)
    (let [[m A start]
          (loop [m     m
                 A     {}
                 start min-node
                 w     min-node]
            (let [[m A start x w d]
                  (loop [m      m
                         A      A
                         start  start
                         x      w
                         next-w (node-get w m :right)
                         d      (node-get x m :degree)]
                    (if-let [y (get A d)]
                      (let [[x y]  (if (not
                                        (compare-priorities min-node m cmp x y))
                                     [y x]
                                     [x y])
                            start  (cond-> start
                                     (= y start) (node-get m :right))
                            next-w (cond-> next-w
                                     (= y next-w) (node-get m :right))
                            m      (check-nodes (link m y x))]
                        (recur m (assoc A d nil) start x next-w (inc d)))
                      [m A start x next-w d]))
                  A (assoc A d x)]
              (if (= w start) [m A start] (recur m A start w))))
          new-min (->> A
                       vals
                       (remove nil?)
                       (reduce
                        (fn [x a]
                          (if (compare-priorities min-node m cmp a x) a x))
                        start))]
      (FibonacciHeap. new-min cmp m))))

(deftype FibonacciHeap [min-node cmp m]
  Heap
    (decrease-priority [this k priority]
      (decrease-priority this k priority false))
    (decrease-priority [_ k priority delete?]
      (when (and (not delete?) (cmp (node-get k m :priority) priority))
        (throw (ex-info "cannot increase priority value"
                        {:new priority
                         :old (node-get k m :priority)
                         :cmp cmp})))
      (let [m        (assoc-in m [k :priority] priority)
            y        (node-get k m :parent)
            m        (if (and y
                              (or delete?
                                  (compare-priorities min-node m cmp k y)))
                       (-> m
                           (cut y k min-node)
                           (check-nodes)
                           (cascading-cut y min-node)
                           (check-nodes))
                       m)
            min-node (if (or delete?
                             (compare-priorities min-node m cmp k min-node))
                       k
                       min-node)]
        (-> min-node
            (FibonacciHeap. cmp m)
            (check-nodes))))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (count m))
    (assoc [this k priority]
      (assert (or (nil? min-node) (contains? m min-node))
              {:min min-node
               :m   m})
      (check-nodes this)
      (if (m k)
        (decrease-priority this k priority)
        (let [this (let [m (assoc m k (make-node k priority))]
                     (if min-node
                       (let [m (-> m
                                   (assoc-in [k :right] min-node)
                                   (assoc-in [k :left]
                                             (node-get min-node m :left))
                                   (assoc-in [min-node :left] k))
                             m (assoc-in m [(node-get k m :left) :right] k)]
                         (if (compare-priorities min-node m cmp k min-node)
                           (FibonacciHeap. k cmp m)
                           (FibonacciHeap. min-node cmp m)))
                       (FibonacciHeap. k cmp m)))]
          (check-nodes this))))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? m k))
    (entryAt [_ k] [(node-get k m :priority) k])
    (seq [_]
      (when-not (nil? min-node)
        (->> m
             (sort-by (comp :priority val))
             (map key))))
    (without [this k]
      (-> this
          (check-nodes)
          (decrease-priority k Integer/MIN_VALUE true)
          (check-nodes)
          pop
          (check-nodes)))
  ILookup
    (valAt [_ k] (node-get k m :priority))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_] (MapEntry. min-node (node-get min-node m :priority)))
    (pop [this]
      (-> this
          (check-nodes)
          (remove-min min-node cmp m)
          (check-nodes)))
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

(defn remove-parents
  [m k]
  (loop [seen #{}
         m    m
         x    (-> k
                  (node-get m :child)
                  (node-get m :right))]
    (when (seen x)
      (throw (ex-info ""
                      {:x x
                       :k k
                       :m m})))
    (let [seen (conj seen x)]
      (if (= x (node-get k m :child))
        m
        (let [m (assoc-in m [x :parent] nil)]
          (recur seen m (node-get x m :right)))))))

(defn remove-min
  [this min-node cmp m]
  (if min-node
    (let [m1 (-> m
                 (cond-> (node-get min-node m :child)
                         (->
                           (assoc-in [(node-get min-node m :child) :parent] nil)
                           (remove-parents min-node)
                           (check-nodes)
                           ((fn [m]
                              (let [min-left     (node-get min-node m :left)
                                    z-child-left (-> min-node
                                                     (node-get m :child)
                                                     (node-get m :left))]
                                (-> m
                                    (assoc-in [min-node :left] z-child-left)
                                    (assoc-in [z-child-left :right] min-node)
                                    (assoc-in [(-> min-node
                                                   (node-get m :child))
                                               :left]
                                              min-left)
                                    (assoc-in [min-left :right]
                                              (node-get min-node m :child))))))
                           (check-nodes)))
                 (check-nodes)
                 ((fn [m]
                    (connect-nodes m
                                   (node-get min-node m :left)
                                   (node-get min-node m :right)))))
          z-right (node-get min-node m1 :right)
          m2 (-> m1
                 (dissoc min-node)
                 (check-nodes))]
      (if (= min-node z-right)
        (FibonacciHeap. nil cmp m2)
        (-> z-right
            (FibonacciHeap. cmp m2)
            (check-nodes)
            consolidate
            (check-nodes))))
    this))

(defmethod print-method FibonacciHeap [o w] (print-method (seq o) w))

(defn make-heap ([cmp] (FibonacciHeap. nil cmp {})) ([] (make-heap <)))

(comment
  (let [queue (-> (make-heap)
                  (assoc :a 1)
                  (assoc :b 2)
                  (assoc :c 3)
                  (assoc :d 0))]
    (reduce
     (fn [v [k _]] (when (= k :a) (println k) (assoc queue :c 1)) (conj v k))
     []
     queue)))
