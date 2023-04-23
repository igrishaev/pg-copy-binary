(ns foo
  (:require
   [pg-copy-binary.core :as core]
   )
  (:import
   org.joda.time.Instant
   org.joda.time.Days
   org.joda.time.LocalTime
   org.joda.time.LocalDate
   org.joda.time.DateTime
   org.joda.time.LocalDateTime))


#_
(* 1000 (.getMillisOfDay -lt))


(extend-protocol core/IPGWrite

  LocalTime

  (-bytes [this]
    (core/arr64 (.getMillisOfDay this)))

  LocalDate

  (-bytes [this]
    (let [days
          (-> (Days/daysBetween (new LocalDate 1970 1 1) this)
              (.getDays)
              (- (.toDays core/PG_EPOCH_DIFF)))]

      (core/arr32 days)))

  DateTime

  #_
  (-bytes [this]
    (arr64
     (+
      (* (- (.getEpochSecond this)
            (.toSeconds PG_EPOCH_DIFF))
         1000 1000)
      (.getNano this))))



  )
