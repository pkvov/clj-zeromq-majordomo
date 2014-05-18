(ns com.infones.zeromq.worker
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

(def ^:private ^:const ^Integer HEARTBEAT_LIVENESS 3)
(def ^:private ^:const ^Integer HEARTBEAT_INTERVAL 2500) ; same as in broker
(def ^:private ^:const ^Integer RECONECT 2500)
(def ^:private ^:const ^Integer HEARTBEAT 2500)
(def ^:private ^:const ^Integer TIMEOUT 2500)

(defrecord Worker [^String broker ^ZMQ$Context ctx ^String service worker heartbeat_at liveness expect_reply ^Boolean verbose reply_to]) 

(defprotocol MajorDomoWorker
  (send-to-broker [^Worker this ^MajorDomo command ^String option ^ZMsg msg])
  (reconnect-to-broker [^Worker this])
  (receive [^Worker this ^ZMsg reply])
  (destroy [^Worker this]))

(def majordomo-worker-implementation 
  {
   :send-to-broker (fn [^Worker this ^MajorDomo command ^String option ^ZMsg msg]
                     (let [^ZMsg n_msg (if-not (nil? msg) (.duplicate msg) (ZMsg. ))]
                       (if-not (nil? option)
                         (.addFirst n_msg (ZFrame. option)))
                       (.addFirst n_msg (new-frame command))
                       (.addFirst n_msg (new-frame W_WORKER))
                       (.addFirst n_msg (ZFrame. (into-array java.lang.Byte/TYPE [])))
                       (if (:verbose this)
                         (do
                           (info "I: sending" command "to broker")
                           (.dump n_msg *out*)))
                       (.send n_msg @(:worker this))))
  
  :reconnect-to-broker (fn [^Worker this]
                         (if-not (nil? @(:worker this))
                           (.destroySocket (:ctx this) @(:worker this)))
                         (reset! (:worker this) (.socket (:ctx this) ZMQ/DEALER))
                         (.connect @(:worker this) (:broker this))
                         (if (:verbose this)
                           (info "I: connecting to broker at " (:broker this)))
                         (send-to-broker this W_READY (:service this) nil)
                         (reset! (:liveness this) HEARTBEAT_LIVENESS)
                         (reset! (:heartbeat_at this) (+ (System/currentTimeMillis ) HEARTBEAT))
                         this)
  
  :receive (fn [^Worker this ^ZMsg reply]
             (assert (or (not (nil? reply))
                         (not @(:expect_reply this))))
             (if-not (nil? reply)
               (do
                 (assert (not (nil? @(:reply_to this))))
                 (.wrap reply @(:reply_to this))
                 (send-to-broker this W_REPLY nil reply)
                 (.destroy reply)))
             (reset! (:expect_reply this) true)
             (let [result (promise )] 
               (while (and
                        (not (realized? result))
                        (not (.isInterrupted (Thread/currentThread))))
                 (let [items (.poller (:ctx this) 1)]
                   (.register items @(:worker this) ZMQ$Poller/POLLIN)
                   (if (= -1 (.poll items TIMEOUT))
                     (throw+ {:type ::INTERUPTER}))
                   (if (true? (.pollin items 0))
                     (let [^ZMsg msg (ZMsg/recvMsg @(:worker this))]
                       (if (nil? msg)
                         (throw+ {:type ::INTERUPTER}))
                       (if (:verbose this)
                         (do
                           (info "I: received message from broker:")
                           (.dump msg *out*)))
                       (reset! (:liveness this) HEARTBEAT_LIVENESS)
                       (assert (and (not (nil? msg))
                                    (>= (.size msg) 3)))
                       (let [^ZFrame empty_ (.pop msg)
                             ^ZFrame header (.pop msg)
                             ^ZFrame command (.pop msg)]
                         (assert (= 0 (alength (.getData empty_))))
                         (.destroy empty_)
                         (assert (frame-equals W_WORKER header))
                         (.destroy header)
                         (cond 
                          (frame-equals W_REQUEST command) (do
                                                             (reset! (:reply_to this) (.unwrap msg))
                                                             (.destroy command)
                                                             (deliver result msg))
                          (frame-equals W_HEARTBEAT command) nil
                          (frame-equals W_HEARTBEAT command) (do
                                                               (reconnect-to-broker this))
                          :else (do 
                                  (info "E: invalid input message: \n")
                                  (.dump msg *out*))))))
                   (if (= 0 (swap! (:liveness this) - 1))
                     (do
                       (if (:verbose this)
                         (info "W: disconnected from broker - retrying\n"))
                       (try
                         (.sleep Thread RECONECT)
                         (catch InterruptedException e
                           (.interrupt (.currentThread Thread))))
                       (reconnect-to-broker this))))
                (if (and (not (realized? result)) 
                         (> (System/currentTimeMillis ) @(:heartbeat_at this)))
                  (do
                    (send-to-broker this W_HEARTBEAT nil nil)
                    (reset! (:heartbeat_at this) (+(System/currentTimeMillis ) HEARTBEAT)))))
               (if (realized? result)
                 @result
                 nil)))
  
  :destroy (fn [^Worker this]
             (.term (:ctx this)))
  })

(extend Worker
  MajorDomoWorker
  majordomo-worker-implementation)

(defn create-worker [^String broker ^String service ^Boolean verbose]
  {:pre [(and (not (nil? broker))
              (not (nil? service)))]}
  (let [worker (->Worker broker 
                       (ZMQ/context 1)
                       service
                       (atom nil)
                       (atom 0)
                       (atom 0)
                       (atom false)
                       verbose
                       (atom nil))]
    (reconnect-to-broker worker)))
