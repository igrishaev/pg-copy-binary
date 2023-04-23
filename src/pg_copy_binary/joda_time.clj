(ns pg-copy-binary.joda-time
  (:require
   [pg-copy-binary.core :as core])
  (:import
   org.joda.time.DateTimeZone
   org.joda.time.Instant
   org.joda.time.Days
   org.joda.time.LocalTime
   org.joda.time.Time ;;
   org.joda.time.Date ;;
   org.joda.time.LocalDate
   org.joda.time.DateTime
   org.joda.time.LocalDateTime))


;;
;; Const
;;

(def ^LocalDate LD_EPOCH
  (new LocalDate 1970 1 1))


(extend-protocol core/IPGWrite

  LocalTime

  (-bytes [this]
    (core/arr64 (.getMillisOfDay this)))

  LocalDate

  (-bytes [this]
    (let [days
          (-> (Days/daysBetween LD_EPOCH this)
              (.getDays)
              (- (.toDays core/PG_EPOCH_DIFF)))]
      (core/arr32 days)))

  LocalDateTime

  (-bytes [this]
    (core/-bytes (.toDateTime this DateTimeZone/UTC)))

  DateTime

  (-bytes [this]

    (let [millis
          (-> this .getMillis )

          sec-millis
          (.getMillisOfSecond this)]

      (core/arr64
       (+ (- millis (.toMillis core/PG_EPOCH_DIFF))
          (* sec-millis 1000))))))
