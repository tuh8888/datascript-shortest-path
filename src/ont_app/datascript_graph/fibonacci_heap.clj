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

(defn node-get [node k] (k @node))

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
  (swap! (node-get other-node :left) assoc :right (node-get other-node :right))
  (swap! (node-get other-node :left) assoc :left (node-get other-node :left))
  (swap! node update :degree dec)
  (if (= (node-get node :degree) 0)
    (swap! node assoc :child nil)
    (when (= (:child node) other-node)
      (swap! node assoc :child (node-get other-node :right))))
  (swap! other-node assoc :right min-node)
  (swap! other-node assoc :left (node-get min-node :left))
  (swap! min-node assoc :left other-node)
  (swap! (node-get other-node :left) assoc :right other-node)
  (swap! other-node assoc :parent nil)
  (swap! other-node assoc :mark false))

(defn cascading-cut
  [node min-node]
  (let [z (node-get node :parent)]
    (when (not (nil? z))
      (if (node-get node :mark)
        (do (cut z node min-node) (cascading-cut z min-node))
        (swap! node assoc :mark true)))))

(defn link
  [node parent]
  (swap! (node-get node :left) assoc :right (node-get node :right))
  (swap! (node-get node :right) assoc :left (node-get node :left))
  (swap! node assoc :parent parent)
  (if (= (node-get parent :child) nil)
    (do (swap! parent assoc :child node)
        (swap! node assoc :right node)
        (swap! node assoc :left node))
    (do (swap! node assoc :left (node-get parent :child))
        (swap! node assoc :right (node-get (node-get parent :child) :right))
        (swap! (node-get parent :child) assoc :right node)
        (swap! (node-get node :right) assoc :left node)))
  (swap! parent update :degree inc)
  (swap! node assoc :mark false))

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
                           next-w (node-get w :right)
                           d      (node-get x :degree)]
                      (if-let [y (get A d)]
                        (let [[x y]  (if (not (compare-priorities cmp x y))
                                       [y x]
                                       [x y])
                              start  (cond-> start
                                       (= y start) (node-get :right))
                              next-w (cond-> next-w
                                       (= y next-w) (node-get :right))]
                          (link y x)
                          (recur (assoc A d nil) start x next-w (inc d)))
                        [A start x next-w d]))
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
                           :old (node-get node :priority)
                           :cmp cmp})))
        (swap! node assoc :priority priority)
        (let [y (node-get node :parent)]
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
                (swap! new-node assoc :left (node-get min-node :left))
                (swap! min-node assoc :left new-node)
                (swap! (node-get new-node :left) assoc :right new-node)
                (if (compare-priorities cmp new-node min-node)
                  (FibonacciHeap. new-node cmp node-map)
                  (FibonacciHeap. min-node cmp node-map)))
            (FibonacciHeap. new-node cmp node-map)))))
    #_(empty [_] (MyFibonacciHeap. (fh/fibonacci-heap) {}))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? node-map k))
    (entryAt [_ k]
      (when-let [node (node-map k)]
        [(node-get node :priority) (node-get node :key)]))
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
        (node-get node :priority)))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_]
      (MapEntry. (node-get min-node :key) (node-get min-node :priority)))
    (pop [this]
      (let [z min-node]
        (if (nil? z)
          this
          (let [node-map (dissoc node-map (node-get min-node :key))]
            (when (node-get z :child)
              (swap! (node-get z :child) assoc :parent nil)
              (let [x (loop [x (-> z
                                   (node-get :child)
                                   (node-get :right))]
                        (if (= x (node-get z :child))
                          x
                          (do (swap! x assoc :parent nil)
                              (recur (node-get x :right)))))])
              (let [min-left     (node-get min-node :left)
                    z-child-left (-> z
                                     (node-get :child)
                                     (node-get :left))]
                (swap! min-node assoc :left z-child-left)
                (swap! z-child-left assoc :right min-node)
                (swap! (node-get z :child) assoc :left min-left)
                (swap! min-left assoc :right (node-get z :child))))
            (swap! (node-get z :left) assoc :right (node-get z :right))
            (swap! (node-get z :right) assoc :left (node-get z :left))
            (if (= z (node-get z :right))
              (FibonacciHeap. nil cmp node-map)
              (-> (FibonacciHeap. (node-get z :right) cmp node-map)
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
