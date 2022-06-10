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

(defn cut
  [m node other-node min-node]
  (let [m (-> m
              (assoc-in [(node-get other-node m :left) :right]
                        (node-get other-node m :right))
              (assoc-in [(node-get other-node m :left) :left]
                        (node-get other-node m :left))
              (update-in [node :degree] dec))
        m (if (= (node-get node m :degree) 0)
            (assoc-in m [node :child] nil)
            (if (= (:child node) other-node)
              (assoc-in m [node :child] (node-get other-node m :right))
              m))]
    (-> m
        (assoc-in [other-node :right] min-node)
        (assoc-in [other-node :left] (node-get min-node m :left))
        (assoc-in [min-node :left] other-node)
        (assoc-in [(node-get other-node m :left) :right] other-node)
        (assoc-in [other-node :parent] nil)
        (assoc-in [other-node :mark] false))))

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

(defn check-node
  [m k]
  (let [vs {:child :parent
            #_#_:parent :child
            :left  :right
            :right :left}]
    (doseq [[v _] vs]
      (let [rev-v     (vs v)
            rev-k     (node-get k m v)
            rev-rev-k (node-get rev-k m rev-v)]
        (when-not (and (nil? rev-rev-k) (#{:child :parent} v))
          (assert (= k rev-rev-k)
                  {:k         k
                   :v         v
                   :rev-v     rev-v
                   :rev-k     rev-k
                   :rev-rev-k rev-rev-k}))))))

(defn check-nodes
  [h]
  (if (instance? FibonacciHeap h)
    (let [m (.m h)]
      (doseq [[k _] m]
        (check-node m k))
      h)
    (let [m h]
      (doseq [[k _] h]
        (check-node h k))
      m)))

(defn connect-nodes
  [m left right]
  (-> m
      (assoc-in [right :left] left)
      (assoc-in [left :right] right)))

(defn link
  [m k parent]
  (let [m (-> m
              (check-nodes)
              ((fn [m]
                 (connect-nodes m (node-get k m :left) (node-get k m :right))))
              #_(assoc-in [(node-get node m :left) :right]
                          (node-get node m :right))
              #_(assoc-in [(node-get node m :right) :left]
                          (node-get node m :left))
              (assoc-in [k :parent] parent))
        m (if (node-get parent m :child)
            (-> m
                ((fn [m] (connect-nodes m k (node-get parent m :child))))
                ((fn [m]
                   (connect-nodes m
                                  (-> parent
                                      (node-get m :child)
                                      (node-get m :right))
                                  k)))
                (check-nodes)
                #_(assoc-in [k :left] (node-get parent m :child))
                #_(assoc-in [k :right]
                            (-> parent
                                (node-get m :child)
                                (node-get m :right)))
                #_(assoc-in [(node-get parent m :child) :right] k)
                #_(assoc-in [(node-get k m :right) :left] k))
            (-> m
                (assoc-in [parent :child] k)
                (connect-nodes k k)
                (check-nodes)
                #_(assoc-in [k :right] k)
                #_(assoc-in [k :left] k)))]
    (-> m
        (update-in [parent :degree] inc)
        (assoc-in [k :mark] false))))

#_(defn add-to-list
    [this l]
    (loop [cur this
           l   l]
      this
      (let [l   (conj l cur)
            l   (if (not (nil? (:child cur))) (add-to-list @(:child cur) l) l)
            cur @(:right cur)]
        (if (= this cur) l (recur cur l)))))



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
                    #_(when (some nil? [start w x next-w])
                        (throw (ex-info ""
                                        {:start  start
                                         :w      w
                                         :x      x
                                         :next-w next-w
                                         :min    min-node})))
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
              (if (not= w start) (recur m A start w) [m A start])))
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
      (assert (contains? m min-node) {:min min-node})
      (let [m        (assoc-in m [k :priority] priority)
            y        (node-get k m :parent)
            m        (if (and (not (nil? y))
                              (or delete?
                                  (compare-priorities min-node m cmp k y)))
                       (-> m
                           (cut y k min-node)
                           (cascading-cut y min-node))
                       m)
            min-node (if (or delete?
                             (compare-priorities min-node m cmp k min-node))
                       k
                       min-node)]
        (-> min-node
            (FibonacciHeap. cmp m)
            check-nodes)))
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
          (decrease-priority k 0 true)
          pop))
  ILookup
    (valAt [_ k] (node-get k m :priority))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_] (MapEntry. min-node (node-get min-node m :priority)))
    (pop [this]
      (check-nodes this)
      (-> this
          (remove-min min-node cmp m)
          #_(check-nodes)))
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
      (if (or (= x (node-get k m :child)) #_(= x k))
        m
        (let [m (assoc-in m [x :parent] nil)]
          (recur seen m (node-get x m :right)))))))

(defn remove-min
  [this min-node cmp m]
  (if min-node
    (let [z-right (node-get min-node m :right)
          m
          (-> m
              (cond->
                (node-get min-node m :child) (-> (assoc-in [(node-get min-node
                                                                      m
                                                                      :child)
                                                            :parent]
                                                           nil)
                                                 (remove-parents min-node)
                                                 (connect-nodes
                                                  min-node
                                                  (-> min-node
                                                      (node-get m :child)
                                                      (node-get m :left)))
                                                 (connect-nodes
                                                  (node-get min-node m :child)
                                                  (node-get min-node m :left))))
              (check-nodes)
              ((fn [m]
                 (connect-nodes m
                                (node-get min-node m :left)
                                (node-get min-node m :right))))
              (dissoc min-node)
              (check-nodes)
              #_(assoc-in [(node-get min-node m :left) :right]
                          (node-get min-node m :right))
              #_(assoc-in [(node-get min-node m :right) :left]
                          (node-get min-node m :left)))]
      (if (= min-node z-right)
        (FibonacciHeap. nil cmp m)
        (let [new-this   (-> z-right
                             (FibonacciHeap. cmp m)
                             (check-nodes)
                             consolidate
                             (check-nodes))
              newer-this (FibonacciHeap. (.min-node new-this)
                                         (.cmp new-this)
                                         m)]
          #_(assert (contains? (.m newer-this) (.min-node newer-this))
                    {:min     (.min-node newer-this)
                     :old-min min-node
                     :right   (node-get min-node m :right)
                     :m       (.m newer-this)})
          newer-this)))
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
