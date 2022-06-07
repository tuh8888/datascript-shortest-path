(ns ont-app.datascript-graph.util)

(defmacro cond-pred->
  "Like cond-> but also threads initial expr through tests."
  {:added "1.5"}
  [expr & clauses]
  (assert (even? (count clauses)))
  (let [g     (gensym)
        steps (map (fn [[test step]]
                     `(if (-> ~g
                              ~test)
                        (-> ~g
                            ~step)
                        ~g))
                   (partition 2 clauses))]
    `(let [~g ~expr
           ~@(interleave (repeat g) (butlast steps))]
       ~(if (empty? steps) g (last steps)))))
