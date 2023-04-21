(ns pg-copy-binary.core
  (:import
   java.io.OutputStream)
  (:gen-class))


(defn arr16 ^bytes [value]
  (byte-array
   [(-> value (bit-and 0xff00) (bit-shift-right 8))
    (-> value (bit-and 0x00ff) (bit-shift-right 0))]))


(defn arr32 ^bytes [value]
  (byte-array
   [(-> value (bit-and 0xff000000) (bit-shift-right 24))
    (-> value (bit-and 0x00ff0000) (bit-shift-right 16))
    (-> value (bit-and 0x0000ff00) (bit-shift-right  8))
    (-> value (bit-and 0x000000ff) (bit-shift-right  0))]))


(defn arr64 ^bytes [value]
  (let [buf
        (-> value
            (BigInteger/valueOf)
            (.toByteArray))

        pad
        (- 8 (alength buf))]

    (if (pos? pad)
      (byte-array (-> []
                      (into (repeat pad 0))
                      (into buf)))
      buf)))


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
  (-bytes ^bytes [this]))


;; PGCOPY\n\377\r\n\0

(def HEADER
   (byte-array 11 (map int [\P \G \C \O \P \Y \newline 0xFF \return \newline 0])))


(extend-protocol IPGWrite

  String

  (-bytes [this]
    (.getBytes this "UTF-8"))

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
    (arr32 this))

  Long

  (-bytes [this]
    (-> (BigInteger/valueOf this)
        (arr64)))

  Float

  (-bytes [this]
    (-> (Float/floatToIntBits this)
        (arr32))))


(defn write [table ^OutputStream out]
  (.write out ^bytes HEADER)
  (.write out (arr32 0))
  (.write out (arr32 0))
  (doseq [row table]
    (.write out (arr16 (count row)))
    (doseq [item row]
      (if (nil? item)
        (.write out (arr32 -1))
        (let [buf
              (-bytes item)]
          (.write out (arr32 (alength buf)))
          (.write out buf)))))
  (.write out (arr16 -1))
  (.close out))


(def table
  [[1 "AFGHANISTAN" nil]
   [2 "ALBANIA" false]
   [3 "ALGERIA" true]]

  #_
  [["AF" "AFGHANISTAN" nil]
   ["AL" "ALBANIA" nil]
   ["DZ" "ALGERIA" nil]
   ["ZM" "ZAMBIA" nil]
   ["ZW" "ZIMBABWE" nil]])


#_
(write table
       (-> "out.bin"
           clojure.java.io/file
           clojure.java.io/output-stream))


(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
