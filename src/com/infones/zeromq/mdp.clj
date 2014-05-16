(ns com.infones.zeromq.mdp
  (import [org.jeromq 
           ZFrame
           ZMQ]))

(defrecord MajorDomo [data])

(defprotocol MajorDomoProtocol
  (new-frame [^MajorDomo this])
  (frame-equals [^MajorDomo this ^ZFrame frame]))

(def majordomo-protocol-impl
  {
   :new-frame (fn [^MajorDomo this]
                (ZFrame. (:data this)))
   
   :frame-equals (fn [^MajorDomo this ^ZFrame frame]
                   (java.util.Arrays/equals (:data this) (.getData frame)))
   })

(extend MajorDomo
  MajorDomoProtocol
  majordomo-protocol-impl)

(defmulti ^:private ->MajorDomo
  (fn [data]
    (class data)))

(defmethod ^:private ->MajorDomo java.lang.String [^String data]
  (MajorDomo. (.getBytes data ZMQ/CHARSET)))

(defmethod ^:private ->MajorDomo java.lang.Long [^Long data]
  (MajorDomo. (into-array Byte/TYPE [data])))

(def C_CLIENT (->MajorDomo "MDPC01"))
(def W_WORKER (->MajorDomo "MDPW01"))
(def W_READY (->MajorDomo 1))
(def W_REQUEST (->MajorDomo 2))
(def W_REPLY (->MajorDomo 3))
(def W_HEARTBEAT (->MajorDomo 4))
(def W_DISCONNECT (->MajorDomo 5))