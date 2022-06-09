(ns ont-app.datascript-graph.fibonacci-heap
  (:import
   (clojure.lang ILookup IPersistentMap IPersistentStack IReduce MapEntry)))

(defrecord ANode [priority parent child right left degree mark])

(defn make-node
  [priority]
  (map->ANode {:priority priority
               :mark     false
               :degree   0
               :left     priority
               :right    priority}))

(defn node-get [k m prop] (get-in m [k prop]))

(defn compare-priorities
  [m cmp k1 k2]
  (->> [k1 k2]
       (map #(node-get % m :priority))
       (apply cmp)))

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

(defn link
  [m node parent]
  (let [m (-> m
              (assoc-in [(node-get node m :left) :right]
                        (node-get node m :right))
              (assoc-in [(node-get node m :right) :left]
                        (node-get node m :left))
              (assoc-in [node :parent] parent))
        m (if (node-get parent m :child)
            (-> m
                (assoc-in [node :left] (node-get parent m :child))
                (assoc-in [node :right]
                          (-> parent
                              (node-get m :child)
                              (node-get m :right)))
                (assoc-in [(node-get parent m :child) :right] node)
                (assoc-in [(node-get node m :right) :left] node))
            (-> m
                (assoc-in [parent :child] node)
                (assoc-in [node :right] node)
                (assoc-in [node :left] node)))]
    (-> m
        (update-in [parent :degree] inc)
        (assoc-in [node :mark] false))))

#_(defn add-to-list
    [this l]
    (loop [cur this
           l   l]
      this
      (let [l   (conj l cur)
            l   (if (not (nil? (:child cur))) (add-to-list @(:child cur) l) l)
            cur @(:right cur)]
        (if (= this cur) l (recur cur l)))))

(defn remove-parents
  [m k]
  (loop [m m
         x (-> k
               (node-get m :child)
               (node-get m :right))]
    (if (= x (node-get k m :child))
      m
      (let [m (-> m
                  (assoc-in [x :parent] nil))]
        (recur m (node-get x m :right))))))

(defprotocol Heap
  (consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(deftype FibonacciHeap [min-node cmp m]
  Heap
    (consolidate [_]
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
                        (let [[x y]  (if (not (compare-priorities m cmp x y))
                                       [y x]
                                       [x y])
                              start  (cond-> start
                                       (= y start) (node-get m :right))
                              next-w (cond-> next-w
                                       (= y next-w) (node-get m :right))
                              m      (link m y x)]
                          (recur m (assoc A d nil) start x next-w (inc d)))
                        [m A start x next-w d]))
                    A (assoc A d x)]
                (if (not= w start) (recur m A start w) [m A start])))]
        (->> A
             vals
             (remove nil?)
             (reduce (fn [this a]
                       (if (compare-priorities m (.cmp this) a (.min-node this))
                         (FibonacciHeap. a cmp (.m this))
                         this))
                     (FibonacciHeap. start cmp m)))))
    (decrease-priority [this k priority]
      (decrease-priority this k priority false))
    (decrease-priority [_ k priority delete?]
      (let [node k]
        (when (and (not delete?) (cmp (node-get node m :priority) priority))
          (throw (ex-info "cannot increase priority value"
                          {:new priority
                           :old (node-get m node :priority)
                           :cmp cmp})))
        (let [m        (assoc-in m [node :priority] priority)
              y        (node-get m node :parent)
              m        (if (and (not (nil? y))
                                (or delete? (compare-priorities m cmp node y)))
                         (-> m
                             (cut y node min-node)
                             (cascading-cut y min-node))
                         m)
              min-node (if (or delete? (compare-priorities m cmp node min-node))
                         node
                         min-node)]
          (FibonacciHeap. min-node cmp m))))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (count m))
    (assoc [this k priority]
      (if (m k)
        (decrease-priority this k priority)
        (let [m (assoc m k (make-node priority))]
          (if min-node
            (let [m (-> m
                        (assoc-in [k :right] min-node)
                        (assoc-in [k :left] (node-get min-node m :left))
                        (assoc-in [min-node :left] k))
                  m (assoc-in m [(node-get k m :left) :right] k)]
              (if (compare-priorities m cmp k min-node)
                (FibonacciHeap. k cmp m)
                (FibonacciHeap. min-node cmp m)))
            (FibonacciHeap. k cmp m)))))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? m k))
    (entryAt [_ k] [(node-get k m :priority) k])
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
    (valAt [_ k] (node-get k m :priority))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_] (MapEntry. min-node (node-get min-node m :priority)))
    (pop [this]
      (let [z min-node]
        (if z
          (let [m (if (node-get z m :child)
                    (let [min-left     (node-get min-node m :left)
                          z-child-left (-> z
                                           (node-get m :child)
                                           (node-get m :left))]
                      (-> m
                          (assoc-in [(node-get z m :child) :parent] nil)
                          (remove-parents z)
                          (assoc-in [min-node :left] z-child-left)
                          (assoc-in [z-child-left :right] min-node)
                          (assoc-in [(node-get z m :child) :left] min-left)
                          (assoc-in [min-left :right] (node-get z m :child))))
                    m)
                m (-> m
                      (assoc-in [(node-get z m :left) :right]
                                (node-get z m :right))
                      (assoc-in [(node-get z m :right) :left]
                                (node-get z m :left)))]
            (if (= z (node-get z m :right))
              (FibonacciHeap. nil cmp (dissoc m min-node))
              (-> z
                  (node-get m :right)
                  (FibonacciHeap. cmp (dissoc m min-node))
                  consolidate)))
          this)))
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
