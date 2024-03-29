(ns ont-app.datascript-graph.fibonacci-heap
  (:require
   [tuh8888.clojure-utils :refer [cond-pred->]])
  (:import (clojure.lang APersistentMap
                         ILookup
                         IPersistentMap
                         IPersistentStack
                         IReduce
                         MapEntry)))

(defrecord Node [priority parent children mark])

(defn make-node
  [priority]
  (map->Node {:priority priority
              :mark     false
              :children #{}}))

(defn node-get [k m prop] (get-in m [k prop]))


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

(defprotocol Heap
  (p< [h k1 k2])
  (link [h k parent])
  (cut [h k parent])
  (cascading-cut [h parent])
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

(deftype FibonacciHeap [min-k cmp roots nodes]
  Checked
    (check-nodes [this]
      (when debug
        (assert (or (and (nil? min-k) (empty? nodes)) (contains? nodes min-k))
                {:min   min-k
                 :nodes (into {} nodes)})
        (doseq [[k _] nodes]
          (check-node nodes k)))
      this)
  Heap
    (p< [_ k1 k2]
      (let [p1 (node-get k1 nodes :priority)
            p2 (node-get k2 nodes :priority)]
        (if (some nil? [p1 p2])
          (throw (ex-info ""
                          {:n1    k1
                           :n2    k2
                           :nodes nodes}))
          (cmp p1 p2))))
    (cascading-cut [this k]
      (let [parent (node-get k nodes :parent)]
        (if (nil? parent)
          this
          (if (node-get k nodes :mark)
            (-> this
                (cut parent k)
                (cascading-cut parent))
            (FibonacciHeap. min-k cmp roots (assoc-in nodes [k :mark] true))))))
    (cut [_ parent k]
      (let [nodes (-> nodes
                      (check-nodes)
                      (update-in [parent :children] disj k)
                      (assoc-in [k :parent] nil)
                      (assoc-in [k :mark] false))
            roots (conj roots k)]
        (FibonacciHeap. min-k cmp roots nodes)))
    (insert [_ k priority]
      (let [nodes         (->> priority
                               make-node
                               (assoc nodes k))
            roots         (conj (or roots #{}) k)
            [min-k nodes] (if min-k
                            (let [min-k (if (p< (FibonacciHeap. min-k
                                                                cmp
                                                                roots
                                                                nodes)
                                                k
                                                min-k)
                                          k
                                          min-k)]
                              [min-k nodes])
                            [k nodes])]
        (-> min-k
            (FibonacciHeap. cmp roots nodes)
            (check-nodes))))
    (link [_ parent k]
      (let [curr-parent (node-get k nodes :parent)
            nodes       (-> nodes
                            (cond-> curr-parent (update-in [curr-parent
                                                            :children]
                                                           disj
                                                           k))
                            (assoc-in [k :parent] parent)
                            (update-in [parent :children] (fnil conj #{}) k)
                            (assoc-in [k :mark] false))
            roots       (disj roots k)]
        (FibonacciHeap. min-k cmp roots nodes)))
    (consolidate [_]
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
                           d      (-> x
                                      (node-get nodes :children)
                                      count)]
                      (if-let [y (get A d)]
                        (let [[k parent]  (if
                                            (not (p< (FibonacciHeap. min-k
                                                                     cmp
                                                                     roots
                                                                     nodes)
                                                     x
                                                     y))
                                            [y x]
                                            [x y])
                              [next-w ws] (if (= parent next-w)
                                            [(first ws) (disj ws x)]
                                            [next-w ws])
                              this        (-> min-k
                                              (FibonacciHeap. cmp roots nodes)
                                              (link k parent)
                                              ((fn [this]
                                                 (try
                                                   (check-nodes this)
                                                   (catch AssertionError e
                                                     (throw
                                                      (ex-info
                                                       (ex-message e)
                                                       (assoc (ex-data e)
                                                              :same-degree-roots
                                                              [k parent]))))))))
                              roots       (.roots this)
                              nodes       (.nodes this)
                              A           (assoc A d nil)
                              d           (inc d)]
                          (recur roots nodes A k ws next-w d))
                        [roots nodes A x ws d]))
                    A (assoc A d x)]
                (if (empty? ws) [roots nodes A] (recur roots nodes A ws))))
            nodes (check-nodes nodes)
            new-min
            (->> A
                 vals
                 (remove nil?)
                 (reduce
                  (fn [x a]
                    (if (p< (FibonacciHeap. min-k cmp roots nodes) a x) a x))
                  (first roots)))]
        (FibonacciHeap. new-min cmp roots nodes)))
    (remove-min [this]
      (if min-k
        (let [children    (node-get min-k nodes :children)
              roots       (into roots children)
              nodes1      (reduce (fn [nodes child]
                                    (assoc-in nodes [child :parent] nil))
                                  nodes
                                  children)
              roots       (disj roots min-k)
              min-k-right (first roots) ;; TODO naive selection of next min-k
              nodes2      (-> nodes1
                              (dissoc min-k)
                              (check-nodes))]
          (if (empty? roots)
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
      (let [parent (node-get k nodes :parent)
            this   (-> nodes
                       (assoc-in [k :priority] priority)
                       (->> (FibonacciHeap. min-k cmp roots))
                       (cond-pred-> #(and parent (or delete? (p< % k parent)))
                                    (-> (cut parent k)
                                        (check-nodes)
                                        (cascading-cut parent)
                                        (check-nodes))))
            nodes  (.nodes this)
            roots  (.roots this)
            min-k  (.min-k this)
            min-k  (if (or delete?
                           (p< (FibonacciHeap. min-k cmp roots nodes) k min-k))
                     k
                     min-k)]
        (-> min-k
            (FibonacciHeap. cmp roots nodes)
            (check-nodes))))
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (count nodes))
    (assoc [this k priority]
      (assert (or (nil? min-k) (contains? nodes min-k))
              {:min   min-k
               :nodes nodes})
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
