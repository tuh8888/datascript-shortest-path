(ns ont-app.datascript-graph.fibonacci-heap
  (:import (clojure.lang ILookup
                         IPersistentMap
                         IPersistentStack
                         IReduce
                         MapEntry
                         Atom)))

(defrecord ANode [parent child right left degree mark])

(defn make-node
  [k x]
  (let [node (atom (map->ANode {:key    k
                                :data   x
                                :mark   false
                                :degree 0}))]
    (swap! node assoc :right node)
    (swap! node assoc :left node)
    node))

(defn compare-keys
  [h n1 n2]
  (let [k1 (cond-> n1
             (instance? Atom n1) (-> deref
                                     :key))
        k2 (cond-> n2
             (instance? Atom n2) (-> deref
                                     :key))]
    ((:cmp @h) k1 k2)))

(defrecord AFibonacciHeap [min cmp])

(defn cut
  [this x min]
  (swap! (:left @x) assoc :right (:right @x))
  (swap! (:left @x) assoc :left (:left @x))
  (swap! this update :degree dec)
  (if (= (:degree @this) 0)
    (swap! this assoc :child nil)
    (when (= (:child this) x) (swap! this assoc :child (:right @x))))
  (swap! x assoc :right min)
  (swap! x assoc :left (:left @min))
  (swap! min assoc :left x)
  (swap! (:left @x) assoc :right x)
  (swap! x assoc :parent nil)
  (swap! x assoc :mark false))

(defn cascading-cut
  [this min]
  (let [z (:parent @this)]
    (when (not (nil? z))
      (if (:mark @this)
        (do (cut z this min) (cascading-cut z min))
        (swap! this assoc :mark true)))))


(defn link
  [this parent]
  (swap! (:left @this) assoc :right (:right @this))
  (swap! (:right @this) assoc :left (:left @this))
  (swap! this assoc :parent parent)
  (if (= (:child @parent) nil)
    (do (swap! parent assoc :child this)
        (swap! this assoc :right this)
        (swap! this assoc :left this))
    (do (swap! this assoc :left (:child @parent))
        (swap! this assoc :right (:right @(:child @parent)))
        (swap! (:child @parent) assoc :right this)
        (swap! (:right @this) assoc :left this)))
  (swap! parent update :degree inc)
  (swap! this assoc :mark false))

#_(defn add-to-list
    [this l]
    (loop [cur this
           l   l]
      (let [l   (conj l cur)
            l   (if (not (nil? (:child cur))) (add-to-list @(:child cur) l) l)
            cur @(:right cur)]
        (if (= this cur) l (recur cur l)))))

(defn consolidate
  [this]
  (let [A     (make-array Atom 45)
        start (atom (:min @this))
        w     (atom (:min @this))]
    (loop []
      (let [x      (atom @w)
            next-w (atom (:right @@w))
            d      (atom (:degree @@x))]
        (while (not (nil? (aget A @d)))
               (let [y (atom (aget A @d))]
                 (when (not (compare-keys this @x @y))
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
    (swap! this assoc :min @start)
    (doseq [a A]
      (when (and (not (nil? a)) (compare-keys this a (:min @this)))
        (swap! this assoc :min a)))))

(defn decrease-key
  ([this x new-data k] (decrease-key this x new-data k false))
  ([this x new-data k delete?]
   (when (and (not delete?) (compare-keys this k x))
     (throw (ex-info "cannot inccrease key value" {})))
   (swap! x assoc :key k)
   (swap! x assoc :data new-data)
   (let [y (:parent @x)]
     (when (and (not (nil? y)) (or delete? (compare-keys this x y)))
       (cut y x (:min @this))
       (cascading-cut y (:min @this)))
     (when (or delete? (compare-keys this x (:min @this)))
       (swap! this assoc :min x)))))

(defn remove-min
  [this]
  (let [z (:min @this)]
    (if (nil? z)
      this
      (do (when (:child @z)
            (swap! (:child @z) assoc :parent nil)
            (let [x (atom (:right @(:child @z)))]
              (while (not= @x (:child @z))
                     (swap! @x assoc :parent nil)
                     (reset! x (:right @@x))))
            (let [min-left     (:left @(:min @this))
                  z-child-left (:left @(:child @z))]
              (swap! (:min @this) assoc :left z-child-left)
              (swap! z-child-left assoc :right (:min @this))
              (swap! (:child @z) assoc :left min-left)
              (swap! min-left assoc :right (:child @z))))
          (swap! (:left @z) assoc :right (:right @z))
          (swap! (:right @z) assoc :left (:left @z))
          (if (= z (:right @z))
            (swap! this assoc :min nil)
            (do (swap! this assoc :min (:right @z)) (consolidate this)))
          #_(set! n (dec n))
          (:data @z)))))


(defn insert
  [this x k]
  (let [node (make-node k x)]
    (if (:min @this)
      (do (swap! node assoc :right (:min @this))
          (swap! node assoc :left (:left @(:min @this)))
          (swap! (:min @this) assoc :left node)
          (swap! (:left @node) assoc :right node)
          (when (compare-keys this node (:min @this))
            (swap! this assoc :min node)))
      (swap! this assoc :min node))
    node))

(defn delete [this x] (decrease-key this x (:data @x) 0 true) (remove-min this))

(defn get-min [this] (:min @this))


(defn fh-node->entry [n] [(:key @n) (:data @n)])

(defn fh-add! [h k v] (insert h v k))

(defn fh-decrease-key! [h n k v] (decrease-key h n v k))

(defn fh-remove! [h n] (delete h n))

(defn fh-node-key [n] (:key @n))

(defn fh-remove-min!
  [h]
  (let [n (get-min h)]
    [(:key @n) (remove-min h)]))

(defn fh-empty? [this] (nil? (:min @this)))

(deftype MyAFibonacciHeap [^AFibonacciHeap h ^:unsynchronized-mutable node-map]
  Object
    (toString [this] (str (.seq this)))
  IPersistentMap
    (count [_] (.size h))
    (assoc [this item priority]
      (if-let [node (node-map item)]
        (do (fh-decrease-key! h node priority item) this)
        (let [node (fh-add! h priority item)]
          (set! node-map (assoc node-map item node))
          this)))
    #_(empty [_] (MyFibonacciHeap. (fh/fibonacci-heap) {}))
    (cons [this [k v]] (assoc this k v))
    (containsKey [_ item] (contains? node-map item))
    (entryAt [_ k]
      (when-let [node (node-map k)]
        (fh-node->entry node)))
    (seq [_]
      (when-not (fh-empty? h)
        (seq [1])
        #_(->> h
               fh-peek-seq
               (map second)
               seq)))
    (without [this item]
      (fh-remove! h (node-map item))
      (set! node-map (dissoc node-map item))
      this)
  ILookup
    (valAt [_ item]
      (when-let [node (node-map item)]
        (fh-node-key node)))
    (valAt [this item not-found] (or (get this item) not-found))
  IPersistentStack
    (peek [_]
      (let [node (get-min h)]
        (MapEntry. (:data @node) (:key @node))))
    (pop [this] (fh-remove-min! h) this)
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
