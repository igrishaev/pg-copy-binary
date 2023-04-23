(ns pg-copy-binary.core
  (:require
   [clojure.java.io :as io])

  (:import
   java.util.Date
   java.util.UUID

   java.time.Instant
   java.time.Duration
   java.time.LocalDate
   java.time.LocalTime
   java.time.ZoneOffset

   java.io.InputStream
   java.io.ByteArrayOutputStream
   java.io.OutputStream
   java.io.Writer))


(def ^Duration PG_EPOCH_DIFF
  (Duration/between Instant/EPOCH
                    (-> (LocalDate/of 2000 1 1)
                        (.atStartOfDay)
                        (.toInstant ZoneOffset/UTC))))


(defn arr16 ^bytes [value]
  (byte-array
   [(-> value (bit-and 0xff00) (bit-shift-right 8))
    (-> value (bit-and 0x00ff))]))


(defn arr32 ^bytes [value]
  (byte-array
   [(-> value (bit-and 0xff000000) (bit-shift-right 24))
    (-> value (bit-and 0x00ff0000) (bit-shift-right 16))
    (-> value (bit-and 0x0000ff00) (bit-shift-right  8))
    (-> value (bit-and 0x000000ff))]))


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


;;
;; Const
;;

(def ^{:tag 'bytes} HEADER
  (byte-array 11 (map int [\P \G \C \O \P \Y \newline 0xFF \return \newline 0])))


(def ^{:tag 'bytes} zero32
  (arr32 0))


(def ^{:tag 'bytes} -one32
  (arr32 -1))


(def ^{:tag 'bytes} -one16
  (arr16 -1))


(defprotocol IPGWrite
  (-bytes ^bytes [this]))


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
     (- (.toEpochDay this)
        (.toDays PG_EPOCH_DIFF))))

  LocalTime

  (-bytes [this]
    (arr64 (quot (.toNanoOfDay this) 1000)))

  Instant

  (-bytes [this]

    (let [seconds
          (- (.getEpochSecond this)
             (.toSeconds PG_EPOCH_DIFF))

          nanos
          (.getNano this)]

      (arr64
       (+ (* seconds 1000 1000)
          (.getNano this)))))

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


(defn table->out [table ^OutputStream out]
  (.write out HEADER)
  (.write out zero32)
  (.write out zero32)
  (doseq [row table]
    (.write out (arr16 (count row)))
    (doseq [item row]
      (if (nil? item)
        (.write out -one32)
        (let [buf
              (-bytes item)]
          (.write out (arr32 (alength buf)))
          (.write out buf)))))
  (.write out -one16)
  (.close out))


(defn table->bytes ^bytes [table]
  (with-open [out (new ByteArrayOutputStream)]
    (table->out table out)
    (.toByteArray out)))


(defn table->file [table ^String path]
  (with-open [out (-> path
                      io/file
                      io/output-stream)]
    (table->out table out)))


(defn table->input-stream ^InputStream [table]
  (-> table
      table->bytes
      io/input-stream))


(defn maps->table [maps row-keys]
  (map (apply juxt row-keys) maps))
