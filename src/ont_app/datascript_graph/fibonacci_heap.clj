(ns ont-app.datascript-graph.fibonacci-heap
  (:require
   [ont-app.datascript-graph.util :refer [cond-pred->]])
  (:import (clojure.lang APersistentMap
                         ILookup
                         IPersistentMap
                         IPersistentStack
                         IReduce
                         MapEntry)))

(defrecord Node [priority parent degree mark])

(defn make-node
  [priority]
  (map->Node {:priority priority
              :mark     false
              :degree   0}))

(defn node-get [k m prop] (get-in m [k prop]))

(defn node-set+
  [nodes prop [k & k-navs] [to & to-navs]]
  (let [k  (reduce #(node-get %1 nodes %2) k k-navs)
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

(def ^:dynamic debug false)

;; For debugging purposes.
(defn check-node
  [nodes k]
  (assert (not (contains? nodes nil)) nodes)
  (let [parent          (node-get k nodes :parent)
        parent-children (-> k
                            (node-get nodes :parent)
                            (node-get nodes :children))
        children        (node-get k nodes :children)]
    (when parent
      (assert (parent-children k)
              {:k     k
               :p     parent
               :pc    parent-children
               :nodes nodes}))
    (doseq [child children]
      (let [cp (node-get child nodes :parent)]
        (assert (= cp k)
                {:k     k
                 :cs    children
                 :c     child
                 :cp    cp
                 :nodes nodes})))))

(defprotocol Checked
  (check-nodes [this]))

(defn cut
  [roots nodes parent k]
  (let [nodes (-> nodes
                  (check-nodes)
                  (update-in [parent :degree] dec)
                  (update-in [parent :children] disj k))
        roots (conj roots k)
        nodes (-> nodes
                  (node-set+ :parent [k] nil)
                  (assoc-in [k :mark] false))]
    [roots nodes]))

(defn cascading-cut
  [roots nodes k min-k]
  (let [parent (node-get k nodes :parent)]
    (if (nil? parent)
      [roots nodes]
      (if (node-get k nodes :mark)
        (let [[roots nodes] (cut roots nodes parent k)]
          (cascading-cut roots nodes parent min-k))
        [roots (assoc-in nodes [k :mark] true)]))))

(defprotocol Heap
  (insert [h k priority])
  (remove-min [h])
  (consolidate [h])
  (decrease-priority [h k priority]
                     [h k priority delete?]))

(extend-protocol Checked
 APersistentMap
   (check-nodes [nodes]
     (when debug
       (doseq [[k _] nodes]
         (check-node nodes k)))
     nodes))

(declare my-consolidate)
(declare my-decrease-priority)

(deftype FibonacciHeap [min-k cmp roots nodes]
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
    (insert [_ k priority]
      (let [nodes         (->> priority
                               make-node
                               (assoc nodes k))
            roots         (conj (or roots #{}) k)
            [min-k nodes] (if min-k
                            (let [#_#_nodes
                                    (-> nodes
                                        (node-set+ :right [k] [min-k])
                                        (node-set+ :left [k] [min-k :left])
                                        (node-set+ :left [min-k] [k])
                                        (node-set+ :right [k :left] [k]))
                                  min-k (if (p< nodes cmp k min-k) k min-k)]
                              [min-k nodes])
                            [k nodes])]
        (-> min-k
            (FibonacciHeap. cmp roots nodes)
            (check-nodes))))
    (consolidate [_] (my-consolidate roots nodes cmp))
    (remove-min [this]
      (if min-k
        (let [children    (node-get min-k nodes :children)
              roots       (into roots children)
              nodes1      (reduce (fn [nodes child]
                                    (node-set+ nodes :parent [child] nil))
                                  nodes
                                  children)
              #_#_nodes1
                (->
                  nodes
                  (cond->
                    child (->
                            (node-set+ :parent [child] nil)
                            (remove-parents min-k)
                            (check-nodes)
                            ((fn [nodes]
                               (let [min-left     (node-get min-k nodes :left)
                                     z-child-left (node-get child nodes :left)]
                                 (-> nodes
                                     (node-set+ :left [child] [min-k :left])
                                     (node-set+ :left [min-k] [z-child-left])
                                     (node-set+ :right [z-child-left] [min-k])
                                     (node-set+ :right [min-left] [child])))))
                            (check-nodes)))
                  (check-nodes)
                  (node-set+ :left [min-k :right] [min-k :left])
                  (node-set+ :right [min-k :left] [min-k :right]))
              roots       (disj roots min-k)
              min-k-right (first roots) ;; TODO naive selection of next min-k
              #_#_min-k-right (node-get min-k nodes1 :right)
              nodes2      (-> nodes1
                              (dissoc min-k)
                              (check-nodes))]
          (if (empty? roots)
            #_(= min-k min-k-right)
            (FibonacciHeap. nil cmp roots nodes2)
            (-> min-k-right
                (FibonacciHeap. cmp roots nodes2)
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
      (my-decrease-priority roots nodes cmp min-k k priority delete?))
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
        (insert this k priority)))
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

(defn my-decrease-priority
  [roots nodes cmp min-k k priority delete?]
  (let [parent        (node-get k nodes :parent)
        [roots nodes] (-> [roots nodes]
                          ((fn [[roots nodes]] [roots
                                                (assoc-in nodes
                                                 [k :priority]
                                                 priority)]))
                          (cond-pred->
                           (->> second
                                (#(p< % cmp k parent))
                                (or delete?)
                                (and parent))
                           (-> ((fn [[roots nodes]] (cut roots nodes parent k)))
                               #_(check-nodes)
                               ((fn [[roots nodes]]
                                  (cascading-cut roots nodes parent min-k)))
                               #_(check-nodes))))
        min-k         (if (or delete? (p< nodes cmp k min-k)) k min-k)]
    (-> min-k
        (FibonacciHeap. cmp roots nodes)
        (check-nodes))))

(defn link
  [roots nodes parent k]
  (let [curr-parent (node-get k nodes :parent)
        nodes       (-> nodes
                        (cond-> curr-parent (update-in [curr-parent :children]
                                                       disj
                                                       k))
                        (node-set+ :parent [k] [parent])
                        (update-in [parent :children] (fnil conj #{}) k)
                        (update-in [parent :degree] inc)
                        (assoc-in [k :mark] false))
        roots       (disj roots k)]
    [roots nodes]))

(defn my-consolidate
  [roots nodes cmp]
  (let [[roots nodes A]
        (loop [roots roots
               nodes nodes
               A     {}
               ws    roots]
          (let [w (first ws)
                ws (disj ws w)
                [roots nodes A x ws d]
                (loop [roots  roots
                       nodes  nodes
                       A      A
                       x      w
                       ws     ws
                       next-w w
                       d      (node-get x nodes :degree)]
                  (if-let [y (get A d)]
                    (let [[k parent]    (if (not (p< nodes cmp x y))
                                          [y x]
                                          [x y])
                          [next-w ws]   (if (= parent next-w)
                                          [(first ws) (disj ws x)]
                                          [next-w ws])
                          [roots nodes] (->
                                          roots
                                          (link nodes k parent)
                                          ((fn [[roots nodes]]
                                             (try (check-nodes nodes)
                                                  [roots nodes]
                                                  (catch AssertionError e
                                                    (throw
                                                     (ex-info
                                                      (ex-message e)
                                                      (assoc (ex-data e)
                                                             :same-degree-roots
                                                             [k parent]))))))))
                          A             (assoc A d nil)
                          d             (inc d)]
                      (recur roots nodes A k ws next-w d))
                    [roots nodes A x ws d]))
                A (assoc A d x)]
            (if (empty? ws) [roots nodes A] (recur roots nodes A ws))))
        nodes (check-nodes nodes)
        new-min (->> A
                     vals
                     (remove nil?)
                     (reduce (fn [x a] (if (p< nodes cmp a x) a x))
                             (first roots)))]
    (FibonacciHeap. new-min cmp roots nodes)))

(defmethod print-method FibonacciHeap [o w] (print-method (seq o) w))

(defn make-heap ([cmp] (FibonacciHeap. nil cmp #{} {})) ([] (make-heap <)))

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
