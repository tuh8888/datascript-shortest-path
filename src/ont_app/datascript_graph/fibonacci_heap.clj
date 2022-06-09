(ns ont-app.datascript-graph.fibonacci-heap
  (:import (clojure.lang ILookup
                         IPersistentMap
                         IPersistentStack
                         IReduce
                         MapEntry
                         Atom)))

(defrecord ANode [priority key parent child right left degree mark])

(defn make-node
  [k x]
  (let [node (atom (map->ANode {:priority k
                                :key      x
                                :mark     false
                                :degree   0}))]
    (swap! node assoc :right node)
    (swap! node assoc :left node)
    node))

(defn compare-priorities
  [h n1 n2]
  (let [p1 (cond-> n1
             (instance? Atom n1) (-> deref
                                     :priority))
        p2 (cond-> n2
             (instance? Atom n2) (-> deref
                                     :priority))]
    ((:cmp @h) p1 p2)))

(defrecord AFibonacciHeap [min cmp])

(defn cut
  [node other-node min-node]
  (swap! (:left @other-node) assoc :right (:right @other-node))
  (swap! (:left @other-node) assoc :left (:left @other-node))
  (swap! node update :degree dec)
  (if (= (:degree @node) 0)
    (swap! node assoc :child nil)
    (when (= (:child node) other-node)
      (swap! node assoc :child (:right @other-node))))
  (swap! other-node assoc :right min-node)
  (swap! other-node assoc :left (:left @min-node))
  (swap! min-node assoc :left other-node)
  (swap! (:left @other-node) assoc :right other-node)
  (swap! other-node assoc :parent nil)
  (swap! other-node assoc :mark false))

(defn cascading-cut
  [node min-node]
  (let [z (:parent @node)]
    (when (not (nil? z))
      (if (:mark @node)
        (do (cut z node min-node) (cascading-cut z min-node))
        (swap! node assoc :mark true)))))

(defn link
  [node parent]
  (swap! (:left @node) assoc :right (:right @node))
  (swap! (:right @node) assoc :left (:left @node))
  (swap! node assoc :parent parent)
  (if (= (:child @parent) nil)
    (do (swap! parent assoc :child node)
        (swap! node assoc :right node)
        (swap! node assoc :left node))
    (do (swap! node assoc :left (:child @parent))
        (swap! node assoc :right (:right @(:child @parent)))
        (swap! (:child @parent) assoc :right node)
        (swap! (:right @node) assoc :left node)))
  (swap! parent update :degree inc)
  (swap! node assoc :mark false))

#_(defn add-to-list
    [this l]
    (loop [cur this
           l   l]
      (let [l   (conj l cur)
            l   (if (not (nil? (:child cur))) (add-to-list @(:child cur) l) l)
            cur @(:right cur)]
        (if (= this cur) l (recur cur l)))))

(defn consolidate
  [h]
  (let [A     (make-array Atom 45)
        start (atom (:min @h))
        w     (atom (:min @h))]
    (loop []
      (let [x      (atom @w)
            next-w (atom (:right @@w))
            d      (atom (:degree @@x))]
        (while (not (nil? (aget A @d)))
               (let [y (atom (aget A @d))]
                 (when (not (compare-priorities h @x @y))
                   (let [temp @y]
                     (reset! y @x)
                     (reset! x temp)))
                 (when (= @y @start) (reset! start (:right @@start)))
                 (when (= @y @next-w) (reset! next-w (:right @@next-w)))
                 (link @y @x)
                 (aset A @d nil)
                 (swap! d inc)))
        (aset A @d @x)
        (reset! w @next-w))
      (when (not= @w @start) (recur)))
    (swap! h assoc :min @start)
    (doseq [a A]
      (when (and (not (nil? a)) (compare-priorities h a (:min @h)))
        (swap! h assoc :min a)))))

(defprotocol Heap
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(deftype MyAFibonacciHeap [^AFibonacciHeap h ^:unsynchronized-mutable node-map]
  Heap
    (decrease-priority [this k priority]
      (decrease-priority this k priority false))
    (decrease-priority [this k priority delete?]
      (let [node (node-map k)]
        (when (and (not delete?)
                   (and (not (compare-priorities h priority node))
                        ;; TODO this check should not be necessary for the tests to pass once immutability is implemented.
                        (not (= priority (:priority @node)))))
          (throw (ex-info "cannot increase priority value"
                          (let [n1 priority
                                n2 node
                                p1 (cond-> n1
                                     (instance? Atom n1) (-> deref
                                                             :priority))
                                p2 (cond-> n2
                                     (instance? Atom n2) (-> deref
                                                             :priority))]
                            {:new     priority
                             :old     (:priority @node)
                             :check   ((:cmp @h) p1 p2)
                             :cmp     (:cmp @h)
                             :k1      p1
                             :k2      p2
                             :delete? delete?}))))
        (swap! node assoc :priority priority)
        (let [y (:parent @node)]
          (when (and (not (nil? y)) (or delete? (compare-priorities h node y)))
            (cut y node (:min @h))
            (cascading-cut y (:min @h)))
          (when (or delete? (compare-priorities h node (:min @h)))
            (swap! h assoc :min node)))
        this))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (.size h))
    (assoc [this k priority]
      (if (node-map k)
        (decrease-priority this k priority)
        (let [new-node (make-node priority k)]
          (if (:min @h)
            (do (swap! new-node assoc :right (:min @h))
                (swap! new-node assoc :left (:left @(:min @h)))
                (swap! (:min @h) assoc :left new-node)
                (swap! (:left @new-node) assoc :right new-node)
                (when (compare-priorities h new-node (:min @h))
                  (swap! h assoc :min new-node)))
            (swap! h assoc :min new-node))
          (set! node-map (assoc node-map k new-node))
          this)))
    #_(empty [_] (MyFibonacciHeap. (fh/fibonacci-heap) {}))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? node-map k))
    (entryAt [_ k]
      (when-let [node (node-map k)]
        [(:priority @node) (:key @node)]))
    (seq [_]
      (when-not (nil? (:min @h))
        (seq [1])
        #_(->> h
               fh-peek-seq
               (map second)
               seq)))
    (without [this k]
      (set! node-map (dissoc node-map k))
      (-> this
          (decrease-priority k 0 true)
          pop))
  ILookup
    (valAt [_ item]
      (when-let [node (node-map item)]
        (:priority @node)))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_]
      (let [node (:min @h)]
        (MapEntry. (:key @node) (:priority @node))))
    (pop [this]
      (let [z (:min @h)]
        (if (nil? z)
          h
          (do (when (:child @z)
                (swap! (:child @z) assoc :parent nil)
                (let [x (atom (:right @(:child @z)))]
                  (while (not= @x (:child @z))
                         (swap! @x assoc :parent nil)
                         (reset! x (:right @@x))))
                (let [min-left     (:left @(:min @h))
                      z-child-left (:left @(:child @z))]
                  (swap! (:min @h) assoc :left z-child-left)
                  (swap! z-child-left assoc :right (:min @h))
                  (swap! (:child @z) assoc :left min-left)
                  (swap! min-left assoc :right (:child @z))))
              (swap! (:left @z) assoc :right (:right @z))
              (swap! (:right @z) assoc :left (:left @z))
              (if (= z (:right @z))
                (swap! h assoc :min nil)
                (do (swap! h assoc :min (:right @z)) (consolidate h)))
              h)))
      this)
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

(defmethod print-method MyAFibonacciHeap [o w] (print-method (seq o) w))

(defn make-heap
  ([cmp]
   (-> nil
       (->AFibonacciHeap cmp)
       atom
       (MyAFibonacciHeap. {})))
  ([] (make-heap <)))

(comment
  (let [queue (-> (make-heap)
                  (assoc :a 1)
                  (assoc :b 2)
                  (assoc :c 3)
                  (assoc :d 0))]
    #_(println queue)
    (reduce
     (fn [v [k _]] (when (= k :a) (println k) (assoc queue :c 1)) (conj v k))
     []
     queue)))
