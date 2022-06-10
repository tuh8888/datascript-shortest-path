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

(defn p<
  [nodes cmp n1 n2]
  (let [p1 (node-get n1 nodes :priority)
        p2 (node-get n2 nodes :priority)]
    (if (some nil? [p1 p2])
      (throw (ex-info ""
                      {:n1 n1
                       :n2 n2
                       :m  nodes}))
      (cmp p1 p2))))

;; For debugging purposes.
(defn check-node
  [nodes k]
  (let [vs {:child  :parent
            :parent :child
            :left   :right
            :right  :left}]
    (doseq [[v _] vs]
      (let [rev-v     (vs v)
            rev-k     (node-get k nodes v)
            rev-rev-k (node-get rev-k nodes rev-v)]
        (if (#{:parent} v)
          (let [c (loop [c #{rev-rev-k}
                         x rev-rev-k]
                    (let [next-x (node-get x nodes :right)]
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
                       :nodes     (-> {}
                                      (into nodes)
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
  [nodes parent k min-k]
  (let [nodes (-> nodes
                  #_(check-nodes)
                  (node-set+ :right [k :left] [k :right])
                  (node-set+ :left [k :right] [k :left])
                  (update-in [parent :degree] dec))
        nodes (if (zero? (node-get parent nodes :degree))
                (node-set+ nodes :child [parent] nil)
                (cond-> nodes
                  (= (node-get parent nodes :child) k) (node-set+ :child
                                                                  [parent]
                                                                  [k :right])))]
    (-> nodes
        (node-set+ :right [k] [min-k])
        (node-set+ :left [k] [min-k :left])
        (node-set+ :left [min-k] [k])
        (node-set+ :right [k :left] [k])
        (node-set+ :parent [k] nil)
        #_(check-nodes)
        (assoc-in [k :mark] false))))

(defn cascading-cut
  [nodes k min-k]
  (let [parent (node-get k nodes :parent)]
    (if (nil? parent)
      nodes
      (if (node-get k nodes :mark)
        (-> nodes
            (cut parent k min-k)
            (cascading-cut parent min-k))
        (assoc-in nodes [k :mark] true)))))

(defn connect-nodes
  [nodes left right]
  (-> nodes
      (assoc-in [right :left] left)
      (assoc-in [left :right] right)))

(defn link
  [nodes k parent]
  (let [nodes (-> nodes
                  #_(check-nodes)
                  (connect-nodes (node-get k nodes :left)
                                 (node-get k nodes :right))
                  (node-set+ :parent [k] [parent]))
        child (node-get parent nodes :child)
        nodes (if child
                (-> nodes
                    (connect-nodes child k)
                    (node-set+ :right [k] [(node-get child nodes :right)])
                    (node-set+ :left [k :right] [k])
                    #_(check-nodes))
                (-> nodes
                    (assoc-in [parent :child] k)
                    (connect-nodes k k)
                    (check-nodes)))]
    (-> nodes
        (update-in [parent :degree] inc)
        (assoc-in [k :mark] false))))

(defprotocol Heap
  (remove-min [h])
  (consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(defn remove-parents
  [nodes k]
  (let [start (node-get k nodes :child)]
    (loop [nodes nodes
           seen  #{}
           child start]
      (let [child (node-get child nodes :right)]
        (when (seen child)
          (throw (ex-info ""
                          {:x child
                           :k k
                           :m nodes})))
        (cond-> nodes
          (not= child start) (-> (assoc-in [child :parent] nil)
                                 (recur (conj seen child) child)))))))

(extend-protocol Checked
 APersistentMap
   (check-nodes [nodes]
     (when debug
       (doseq [[k _] nodes]
         (check-node nodes k)))
     nodes))

(deftype FibonacciHeap [min-k cmp nodes]
  Checked
    (check-nodes [this]
      (when debug
        (assert (or (and (nil? min-k) (empty? nodes)) (contains? nodes min-k))
                {:min min-k
                 :m   (into {} nodes)})
        (doseq [[k _] nodes]
          (check-node nodes k)))
      this)
  Heap
    (consolidate [_]
      (let [[nodes A start]
            (loop [nodes nodes
                   A     {}
                   start min-k
                   w     min-k]
              (let [[m A start x w d]
                    (loop [nodes  nodes
                           A      A
                           start  start
                           x      w
                           next-w (node-get w nodes :right)
                           d      (node-get x nodes :degree)]
                      (if-let [y (get A d)]
                        (let [[x y]  (if (not (p< nodes cmp x y)) [y x] [x y])
                              start  (cond-> start
                                       (= y start) (node-get nodes :right))
                              next-w (cond-> next-w
                                       (= y next-w) (node-get nodes :right))
                              nodes  (-> nodes
                                         (link y x)
                                         (check-nodes))]
                          (recur nodes (assoc A d nil) start x next-w (inc d)))
                        [nodes A start x next-w d]))
                    A (assoc A d x)]
                (if (= w start) [m A start] (recur m A start w))))
            new-min (->> A
                         vals
                         (remove nil?)
                         (reduce (fn [x a] (if (p< nodes cmp a x) a x)) start))]
        (FibonacciHeap. new-min cmp nodes)))
    (remove-min [this]
      (if min-k
        (let [child       (node-get min-k nodes :child)
              nodes1      (->
                            nodes
                            (cond->
                              child (-> (assoc-in [child :parent] nil)
                                        (remove-parents min-k)
                                        (check-nodes)
                                        ((fn [nodes]
                                           (let [min-left     (node-get min-k
                                                                        nodes
                                                                        :left)
                                                 z-child-left (node-get child
                                                                        nodes
                                                                        :left)]
                                             (-> nodes
                                                 (node-set+ :left
                                                            [child]
                                                            [min-k :left])
                                                 (node-set+ :left
                                                            [min-k]
                                                            [z-child-left])
                                                 (node-set+ :right
                                                            [z-child-left]
                                                            [min-k])
                                                 (node-set+ :right
                                                            [min-left]
                                                            [child])))))
                                        (check-nodes)))
                            (check-nodes)
                            (node-set+ :left [min-k :right] [min-k :left])
                            (node-set+ :right [min-k :left] [min-k :right]))
              min-k-right (node-get min-k nodes1 :right)
              nodes2      (-> nodes1
                              (dissoc min-k)
                              (check-nodes))]
          (if (= min-k min-k-right)
            (FibonacciHeap. nil cmp nodes2)
            (-> min-k-right
                (FibonacciHeap. cmp nodes2)
                (check-nodes)
                consolidate
                (check-nodes))))
        this))
    (decrease-priority [this k priority]
      (decrease-priority this k priority false))
    (decrease-priority [_ k priority delete?]
      (when (and (not delete?) (cmp (node-get k nodes :priority) priority))
        (throw (ex-info "cannot increase priority value"
                        {:new priority
                         :old (node-get k nodes :priority)
                         :cmp cmp})))
      (let [nodes  (assoc-in nodes [k :priority] priority)
            parent (node-get k nodes :parent)
            nodes  (if (and parent (or delete? (p< nodes cmp k parent)))
                     (-> nodes
                         (cut parent k min-k)
                         (check-nodes)
                         (cascading-cut parent min-k)
                         (check-nodes))
                     nodes)
            min-k  (if (or delete? (p< nodes cmp k min-k)) k min-k)]
        (-> min-k
            (FibonacciHeap. cmp nodes)
            (check-nodes))))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (count nodes))
    (assoc [this k priority]
      (assert (or (nil? min-k) (contains? nodes min-k))
              {:min min-k
               :m   nodes})
      (check-nodes this)
      (if (contains? nodes k)
        (decrease-priority this k priority)
        (let [nodes         (assoc nodes k (make-node k priority))
              [min-k nodes] (if min-k
                              (let [nodes (-> nodes
                                              (assoc-in [k :right] min-k)
                                              (assoc-in [k :left]
                                                        (node-get min-k
                                                                  nodes
                                                                  :left))
                                              (assoc-in [min-k :left] k)
                                              (node-set+ :right [k :left] [k]))
                                    min-k (if (p< nodes cmp k min-k) k min-k)]
                                [min-k nodes])
                              [k nodes])]
          (-> min-k
              (FibonacciHeap. cmp nodes)
              (check-nodes)))))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ k] (contains? nodes k))
    (entryAt [_ k] [(node-get k nodes :priority) k])
    (seq [_]
      (when-not (nil? min-k)
        (->> nodes
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
    (valAt [_ k] (node-get k nodes :priority))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_] (MapEntry. min-k (node-get min-k nodes :priority)))
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
