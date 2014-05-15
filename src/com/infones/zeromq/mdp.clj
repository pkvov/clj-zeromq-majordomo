(ns com.infones.zeromq.mdp
  (import [org.jeromq 
           ZFrame
           ZMQ]))

(defprotocol MajorDomoProtocol
  (new-frame [this])
  (frame-equals [this ^ZFrame frame]))

(defrecord MajorDomo [data]
  MajorDomoProtocol
  (new-frame [this]
    (ZFrame. data))
  (frame-equals [this frame]
    (java.util.Arrays/equals data (.getData frame))))

(defmulti ^:private ->MajorDomo
  (fn [data]
    (class data)))

(defmethod ^:private ->MajorDomo java.lang.String [data]
  (MajorDomo. (.getBytes data ZMQ/CHARSET)))

(defmethod ^:private ->MajorDomo java.lang.Long [data]
  (MajorDomo. (into-array Byte/TYPE [data])))

(def C_CLIENT (->MajorDomo "MDPC01"))
(def W_WORKER (->MajorDomo "MDPW01"))
(def W_READY (->MajorDomo 1))
(def W_REQUEST (->MajorDomo 2))
(def W_REPLY (->MajorDomo 3))
(def W_HEARTBEAT (->MajorDomo 4))
(def W_DISCONNECT (->MajorDomo 5))