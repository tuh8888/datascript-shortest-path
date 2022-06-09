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
  [cmp n1 n2]
  (let [p1 (cond-> n1
             (instance? Atom n1) (-> deref
                                     :priority))
        p2 (cond-> n2
             (instance? Atom n2) (-> deref
                                     :priority))]
    (cmp p1 p2)))

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


(defprotocol Heap
  (consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(deftype FibonacciHeap [min-node cmp node-map]
  Heap
    (consolidate [_]
      (let [[A start]
            (loop [A     {}
                   start min-node
                   w     min-node]
              (let [[A start x w d]
                    (loop [A      A
                           start  start
                           x      w
                           next-w (:right @w)
                           d      (:degree @x)]
                      (if (nil? (get A d))
                        [A start x next-w d]
                        (let [y      (get A d)
                              [x y]  (if (not (compare-priorities cmp x y))
                                       [y x]
                                       [x y])
                              start  (if (= y start) (:right @start) start)
                              next-w (if (= y next-w) (:right @next-w) next-w)]
                          (link y x)
                          (recur (assoc A d nil) start x next-w (inc d)))))
                    A (assoc A d x)]
                (if (not= w start) (recur A start w) [A start])))]
        (->> A
             vals
             (remove nil?)
             (reduce (fn [this a]
                       (if (compare-priorities (.cmp this) a (.min-node this))
                         (FibonacciHeap. a cmp node-map)
                         this))
                     (FibonacciHeap. start cmp node-map)))))
    (decrease-priority [this k priority]
      (decrease-priority this k priority false))
    (decrease-priority [this k priority delete?]
      (let [node (node-map k)]
        (when (and (not delete?) (compare-priorities cmp node priority))
          (throw (ex-info "cannot increase priority value"
                          {:new priority
                           :old (:priority @node)
                           :cmp cmp})))
        (swap! node assoc :priority priority)
        (let [y (:parent @node)]
          (when (and (not (nil? y))
                     (or delete? (compare-priorities cmp node y)))
            (cut y node min-node)
            (cascading-cut y min-node))
          (if (or delete? (compare-priorities cmp node min-node))
            (FibonacciHeap. node cmp node-map)
            this))))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (count node-map))
    (assoc [this k priority]
      (if (node-map k)
        (decrease-priority this k priority)
        (let [new-node (make-node priority k)
              node-map (assoc node-map k new-node)]
          (if min-node
            (do (swap! new-node assoc :right min-node)
                (swap! new-node assoc :left (:left @min-node))
                (swap! min-node assoc :left new-node)
                (swap! (:left @new-node) assoc :right new-node)
                (if (compare-priorities cmp new-node min-node)
                  (FibonacciHeap. new-node cmp node-map)
                  (FibonacciHeap. min-node cmp node-map)))
            (FibonacciHeap. new-node cmp node-map)))))
    #_(empty [_] (MyFibonacciHeap. (fh/fibonacci-heap) {}))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? node-map k))
    (entryAt [_ k]
      (when-let [node (node-map k)]
        [(:priority @node) (:key @node)]))
    (seq [_]
      (when-not (nil? min-node)
        (seq [1])
        #_(->> h
               fh-peek-seq
               (map second)
               seq)))
    (without [this k]
      (-> this
          (decrease-priority k 0 true)
          pop))
  ILookup
    (valAt [_ item]
      (when-let [node (node-map item)]
        (:priority @node)))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_] (MapEntry. (:key @min-node) (:priority @min-node)))
    (pop [this]
      (let [z min-node]
        (if (nil? z)
          this
          (let [node-map (dissoc node-map (:key @min-node))]
            (when (:child @z)
              (swap! (:child @z) assoc :parent nil)
              (let [x (atom (:right @(:child @z)))]
                (while (not= @x (:child @z))
                       (swap! @x assoc :parent nil)
                       (reset! x (:right @@x))))
              (let [min-left     (:left @min-node)
                    z-child-left (:left @(:child @z))]
                (swap! min-node assoc :left z-child-left)
                (swap! z-child-left assoc :right min-node)
                (swap! (:child @z) assoc :left min-left)
                (swap! min-left assoc :right (:child @z))))
            (swap! (:left @z) assoc :right (:right @z))
            (swap! (:right @z) assoc :left (:left @z))
            (if (= z (:right @z))
              (FibonacciHeap. nil cmp node-map)
              (-> (FibonacciHeap. (:right @z) cmp node-map)
                  consolidate))))))
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
    #_(println queue)
    (reduce
     (fn [v [k _]] (when (= k :a) (println k) (assoc queue :c 1)) (conj v k))
     []
     queue)))
