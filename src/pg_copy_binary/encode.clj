(ns pg-copy-binary.encode
  (:require
   [pg.oid :as oid])
)


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


(defmulti -encode
  (fn [value oid _]
    [(type value) oid]))


(defmethod -encode [String oid/TEXT]
  [^String value _ _]
  (.getBytes value "UTF-8"))


(defmethod -encode [Character oid/TEXT]
  [^Character value oid opt]
  (-encode (str val) oid opt))


;;
;; Long
;;

(defmethod -encode [Long oid/INT8]
  [^Long value _ _]
  (arr64 value))


(defmethod -encode [Long oid/INT4]
  [^Long value oid opt]
  (-encode (int value) oid opt))


(defmethod -encode [Long oid/INT2]
  [^Long value oid opt]
  (-encode (short value) oid opt))


;;
;; Integer
;;

(defmethod -encode [Integer oid/INT8]
  [^Integer value oid opt]
  (-encode (long value) oid opt))


(defmethod -encode [Integer oid/INT4]
  [^Integer value oid opt]
  (arr32 value))


(defmethod -encode [Integer oid/INT2]
  [^Integer value oid opt]
  (-encode (short value) oid opt))


;;
;; Short
;;

(defmethod -encode [Short oid/INT8]
  [^Short value oid opt]
  (-encode (long value) oid opt))


(defmethod -encode [Short oid/INT4]
  [^Short value oid opt]
  (-encode (int value) oid opt))


(defmethod -encode [Short oid/INT2]
  [^Short value oid opt]
  (arr16 value))


;;
;; Bool
;;

(defmethod -encode [Boolean oid/BOOL]
  [^Boolean value _ _]
  (let [b
        (case value
          true 1
          false 0)]
    (byte-array [b])))


;;
;; Float
;;

(defmethod -encode [Float oid/FLOAT4]
  [^Float value oid opt]
  (-> (Float/floatToIntBits value)
      (arr32)))


(defmethod -encode [Float oid/FLOAT8]
  [^Float value oid opt]
  (-encode (double value) oid opt))


;;
;; Double
;;

(defmethod -encode [Double oid/FLOAT4]
  [^Double value oid opt]
  (-encode (float value) oid opt))


(defmethod -encode [Double oid/FLOAT8]
  [^Double value oid opt]
  (-> (Double/longBitsToDouble value)
      (arr64)))


(def defaults
  {Double oid/FLOAT8
   Float  oid/FLOAT4
   String oid/TEXT
   Long   oid/INT8

   }
  )
