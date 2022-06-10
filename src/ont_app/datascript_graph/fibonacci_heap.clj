(ns ont-app.datascript-graph.fibonacci-heap
  (:import (clojure.lang ILookup
                         APersistentMap
                         IPersistentMap
                         IPersistentStack
                         IReduce
                         MapEntry)))

(defrecord Node [priority parent child right left degree mark])

(defn make-node
  [k priority]
  (map->Node {:priority priority
              :mark     false
              :degree   0
              :left     k
              :right    k}))

(defn node-get [k m prop] (get-in m [k prop]))

(defn node-set+
  [nodes prop [k & k-navs] [to & to-navs]]
  (let [k
        #_(cond-> k
            (coll? k) ((fn [k & k-navs] (reduce #(node-get %1 m %2) k k-navs))))
        (reduce #(node-get %1 nodes %2) k k-navs)
        to (reduce #(node-get %1 nodes %2) to to-navs)]
    (assoc-in nodes [k prop] to)))

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

;; For debugging purposes.
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

(defprotocol Checked
  (check-nodes [this]))


(defn cut
  [m node x min-node]
  (let [m (-> m
              #_(check-nodes)
              (node-set+ :right [x :left] [x :right])
              (node-set+ :left [x :right] [x :left])
              (update-in [node :degree] dec))
        m (if (zero? (node-get node m :degree))
            (node-set+ m :child [node] nil)
            (cond-> m
              (= (node-get node m :child) x) (node-set+ :child
                                                        [node]
                                                        [x :right])))]
    (-> m
        (node-set+ :right [x] [min-node])
        (node-set+ :left [x] [min-node :left])
        (node-set+ :left [min-node] [x])
        (node-set+ :right [x :left] [x])
        (node-set+ :parent [x] nil)
        #_(check-nodes)
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
              #_(check-nodes)
              (connect-nodes (node-get k m :left) (node-get k m :right))
              (node-set+ :parent [k] [parent]))
        m (if (node-get parent m :child)
            (-> m
                (connect-nodes (node-get parent m :child) k)
                (node-set+ :right
                           [k]
                           [(-> parent
                                (node-get m :child)
                                (node-get m :right))])
                (node-set+ :left [k :right] [k])
                #_(check-nodes))
            (-> m
                (assoc-in [parent :child] k)
                (connect-nodes k k)
                (check-nodes)))]
    (-> m
        (update-in [parent :degree] inc)
        (assoc-in [k :mark] false))))

(defprotocol Heap
  (remove-min [h])
  (consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

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

(extend-protocol Checked
 APersistentMap
   (check-nodes [nodes]
     (when debug
       (doseq [[k _] nodes]
         (check-node nodes k)))
     nodes))

(deftype FibonacciHeap [min-node cmp m]
  Checked
    (check-nodes [this]
      (when debug
        (assert (or (and (nil? min-node) (empty? m)) (contains? m min-node))
                {:min min-node
                 :m   (into {} m)})
        (doseq [[k _] m]
          (check-node m k)))
      this)
  Heap
    (consolidate [this]
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
                        (let [[x y]  (if (not (compare-priorities min-node
                                                                  m
                                                                  cmp
                                                                  x
                                                                  y))
                                       [y x]
                                       [x y])
                              start  (cond-> start
                                       (= y start) (node-get m :right))
                              next-w (cond-> next-w
                                       (= y next-w) (node-get m :right))
                              m      (-> m
                                         (link y x)
                                         (check-nodes))]
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
        (FibonacciHeap. new-min cmp m)))
    (remove-min [this]
      (if min-node
        (let [child   (node-get min-node m :child)
              m1      (-> m
                          (cond->
                            child (-> (assoc-in [child :parent] nil)
                                      (remove-parents min-node)
                                      (check-nodes)
                                      ((fn [m]
                                         (let [min-left     (node-get min-node
                                                                      m
                                                                      :left)
                                               z-child-left (node-get child
                                                                      m
                                                                      :left)]
                                           (-> m
                                               (node-set+ :left
                                                          [child]
                                                          [min-node :left])
                                               (node-set+ :left
                                                          [min-node]
                                                          [z-child-left])
                                               (node-set+ :right
                                                          [z-child-left]
                                                          [min-node])
                                               (node-set+ :right
                                                          [min-left]
                                                          [child])))))
                                      (check-nodes)))
                          (check-nodes)
                          (node-set+ :left [min-node :right] [min-node :left])
                          (node-set+ :right [min-node :left] [min-node :right]))
              z-right (node-get min-node m1 :right)
              m2      (-> m1
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
      (if (contains? m k)
        (decrease-priority this k priority)
        (let [m            (assoc m k (make-node k priority))
              [min-node m] (if min-node
                             (let [m        (-> m
                                                (assoc-in [k :right] min-node)
                                                (assoc-in [k :left]
                                                          (node-get min-node
                                                                    m
                                                                    :left))
                                                (assoc-in [min-node :left] k)
                                                (node-set+ :right
                                                           [k :left]
                                                           [k]))
                                   min-node (if (compare-priorities min-node
                                                                    m
                                                                    cmp
                                                                    k
                                                                    min-node)
                                              k
                                              min-node)]
                               [min-node m])
                             [k m])]
          (-> min-node
              (FibonacciHeap. cmp m)
              (check-nodes)))))
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
          remove-min
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
