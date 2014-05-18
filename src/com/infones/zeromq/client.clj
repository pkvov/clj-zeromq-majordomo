(ns com.infones.zeromq.client
  (:use com.infones.zeromq.mdp
        [slingshot.slingshot :only [throw+]]
        [taoensso.timbre :only [info]])
  (:import [org.jeromq 
            ZMQ$Context
            ZFrame
            ZMsg
            ZMQ
            ZMQ$Poller
            ZMQ$Socket]
           [com.infones.zeromq.mdp
            MajorDomo]))

(def ^:const ^:private ^long TIMEOUT 2500)

(defrecord Client [^ZMQ$Context ctx ^ZMQ$Socket client ^Boolean verbose])

(defprotocol MajorDomoClient
  (recv [^Client this])
  (send-request [^Client this ^String service ^ZMsg request])
  (destroy [^Client this]))

(def majordomo-client
  {
   :recv (fn [^Client this]
           (let [^ZMQ$Poller items (.poller (:ctx this) 1)]
             (.register items (:client this) ZMQ$Poller/POLLIN)
             (if (= -1 (.poll items (* 1000 TIMEOUT)))
               (throw+ {:type ::INTERUPTER}))
             (if (true? (.pollin items 0))
               (let [^ZMsg msg (ZMsg/recvMsg (:client this))]
                 (if (:verbose this)
                   (do 
                     (info "I: received reply:")
                     (.dump msg *out*)))
                 (assert (>= 4 (.size msg)))
                 (let [^ZFrame empty (.pop msg)
                       ^ZFrame header (.pop msg)
                       ^ZFrame reply_service (.pop msg)]
                   (assert (= 0 (alength (.getData empty))))
                   (.destroy empty)
                   (assert (frame-equals C_CLIENT header))
                   (.destroy header)
                   (.destroy reply_service)
                   msg)))))
  
  :send-request (fn [this ^String service ^ZMsg request]
                  (assert (not (nil? request)))
                  (assert (not (nil? service)))
                  (.addFirst request service)
                  (.addFirst request (new-frame C_CLIENT))
                  (.addFirst request "")
                  (if (:verbose this)
                    (do 
                      (info "I: send request to '" service "' service: \n")
                      (.dump request *out*)))
                  (.send request (:client this)))
  
  :destroy (fn [^Client this]
             (.term (:ctx this)))})

(extend Client
  MajorDomoClient
  majordomo-client)

(defn create-client [^String broker ^Boolean verbose]
  (let [^ZMQ$Context ctx (ZMQ/context 1)
        client (.socket ctx ZMQ/DEALER)]
    (.connect client broker)
    (if verbose
      (info "I: connecting to broker at ", broker))
    (->Client ctx client verbose)))






