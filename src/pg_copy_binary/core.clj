(ns pg-copy-binary.core
  (:import
   java.io.OutputStream
   )
  (:gen-class))


#_
(-> 234234324234123
    (bit-and 0xff000000)
    (bit-shift-right 24))


(defn arr-concat ^bytes [^bytes bytes1 ^bytes bytes2]
  (let [result
        (byte-array (+ (alength bytes1) (alength bytes2)))]
    (System/arraycopy bytes1 0 result 0                (alength bytes1))
    (System/arraycopy bytes2 0 result (alength bytes1) (alength bytes2))
    result))


(defn arr-append ^bytes [^bytes buf b]
  (arr-concat buf (byte-array 1 [b])))


(defn arr-left-pad [^bytes buf total]
  (let [diff
        (- total (alength buf))]

    (if (pos? diff)
      (arr-concat (byte-array diff) buf)
      buf)))


(defprotocol IPGWrite
  (-bytes [this])
  (-byte-len [this])
  (-write [this ^OutputStream out]))


(def HEADER
  (byte-array 1)
  )


(extend-protocol IPGWrite

  String

  (-bytes [this]
    (-> this
        (.getBytes "UTF-8")
        (arr-append 0)))

  Character

  (-bytes [this]
    (-bytes (str this)))

  clojure.lang.Symbol

  (-bytes [this]
    (-bytes (str this)))

  clojure.lang.Keyword

  (-bytes [this]
    (-bytes (-> this str (subs 1))))

  Boolean

  (-bytes [this]
    (let [b
          (case this
            true 1
            false 0)]
      (byte-array [b])))

  Integer

  (-bytes [this]
    (-> (BigInteger/valueOf this)
        (.toByteArray)
        (arr-left-pad 4)))

  Long

  (-bytes [this]
    (-> (BigInteger/valueOf this)
        (.toByteArray)
        (arr-left-pad 8)))

  Float

  (-bytes [this]
    (-> (Float/floatToIntBits this)
        (BigInteger/valueOf)
        (.toByteArray)
        (arr-left-pad 4))
    )





  )



(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
