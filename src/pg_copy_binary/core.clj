(ns pg-copy-binary.core
  (:import
   java.util.Date
   java.util.UUID

   java.time.ZonedDateTime ;;
   java.time.Instant
   java.time.Duration
   java.time.LocalDate
   java.time.LocalTime
   java.time.ZoneOffset

   java.io.InputStream
   java.io.ByteArrayOutputStream
   java.io.OutputStream
   java.io.Writer)
  (:gen-class))


(def ^Duration PG_EPOCH_DIFF
  (Duration/between Instant/EPOCH
                    (-> (LocalDate/of 2000 1 1)
                        (.atStartOfDay)
                        (.toInstant ZoneOffset/UTC))))


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

  UUID

  (-bytes [this]

    (let [most-bits
          (.getMostSignificantBits this)

          least-bits
          (.getLeastSignificantBits this)]

      (byte-array
       (-> []
           (into (arr64 most-bits))
           (into (arr64 least-bits))))))

  LocalDate

  (-bytes [this]
    (arr32
     (-
      (.toEpochDay this)
      (.toDays PG_EPOCH_DIFF))))

  LocalTime

  (-bytes [this]
    (arr64 (quot (.toNanoOfDay this) 1000)))

  Instant

  (-bytes [this]
    (arr64
     (+
      (* (- (.getEpochSecond this)
            (.toSeconds PG_EPOCH_DIFF))
         1000 1000)
      (.getNano this))))

  Date

  (-bytes [this]
    (-bytes (.toInstant this)))

  Boolean

  (-bytes [this]
    (let [b
          (case this
            true 1
            false 0)]
      (byte-array [b])))

  Short

  (-bytes [this]
    (arr16 this))

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
        (arr32)))

  Double

  (-bytes [this]
    (-> (Double/longBitsToDouble this)
        (arr64))))


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


(defn row->bytes [row]
  (let [out (new ByteArrayOutputStream)]
    (doseq [item row]
      (if (nil? item)
        (.write out (arr32 -1))
        (let [buf (-bytes item)]
          (.write out (arr32 (alength buf)))
          (.write out buf))))
    (.toByteArray out)))


(defn write-seq [table]
  (let [[row & rows] table]
    (when row
      (lazy-cat (row->bytes row) (write-seq rows)))))


(def table

  [[(new Date) #_(Instant/now)]]

  #_
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
(def big-table
  (for [x (range 1 (* 1000 1000))]
    [x (format "%20d" x) (> (rand) 0.5)]))

#_
(def result
  (write big-table
         (-> "out.bin"
             clojure.java.io/file
             clojure.java.io/output-stream)))

#_
(def result
  (write-csv big-table
             (-> "out.csv"
                 clojure.java.io/file
                 clojure.java.io/writer)))


(defn write-csv [table ^Writer out]
  (doseq [row table]
    (.write out (->> row
                     (map str)
                     (clojure.string/join ",")))
    (.write out "\n"))
  (.close out))


(defn ->input-stream [table]
  (proxy [InputStream] []
    (read [^bytes buf]
      (println buf)
      42)))



(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
