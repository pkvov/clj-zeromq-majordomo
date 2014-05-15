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
            ZMQ$Socket]))

(def ^:private ^:const ^Integer HEARTBEAT_LIVENESS 3)
(def ^:private ^:const ^Integer HEARTBEAT_INTERVAL 2500) ; same as in broker
(def ^:private ^:const ^Integer RECONECT 2500)
(def ^:private ^:const ^Integer HEARTBEAT 2500)
(def ^:private ^:const ^Integer TIMEOUT 2500)

(defprotocol MajorDomoWorker
  (send-to-broker [this command ^String option ^ZMsg msg])
  (reconnect-to-broker [this])
  (receive [this ^ZMsg reply])
  (destroy [this]))

(defrecord Worker [^String broker ^ZMQ$Context ctx ^String service worker heartbeat_at liveness expect_reply ^Boolean verbose reply_to] ;heartbeat_at, liveness, expect_reply, reply-to must be atom
  MajorDomoWorker
  (send-to-broker [this command option msg]
    (let [n_msg (if-not (nil? msg) (.duplicate msg) (ZMsg. ))]
      (if-not (nil? option)
        (.addFirst n_msg (ZFrame. option)))
      (.addFirst n_msg (new-frame command))
      (.addFirst n_msg (new-frame W_WORKER))
      (.addFirst n_msg (ZFrame. (into-array java.lang.Byte/TYPE [])))
      (if verbose
        (do
          (info "I: sending" command "to broker")
          (.dump n_msg *out*)))
      (.send n_msg @worker)))
  
  (reconnect-to-broker [this]
    (if-not (nil? @worker)
      (.destroySocket ctx @worker))
    (reset! worker (.socket ctx ZMQ/DEALER))
    (.connect @worker broker)
    (if verbose
      (info "I: connecting to broker at " broker))
    (send-to-broker this W_READY service nil)
    (reset! liveness HEARTBEAT_LIVENESS)
    (reset! heartbeat_at (+ (System/currentTimeMillis ) HEARTBEAT))
    this)
  
  (receive [this reply]
    (assert (or (not (nil? reply))
                (not @expect_reply)))
    (if-not (nil? reply)
      (do
        (assert (not (nil? @reply_to)))
        (.wrap reply @reply_to)
        (send-to-broker this W_REPLY nil reply)
        (.destroy reply)))
    (reset! expect_reply true)
    (let [result (promise )] 
      (while (and
               (not (realized? result))
               (not (.isInterrupted (Thread/currentThread))))
       (let [items (.poller ctx 1)]
         (.register items @worker ZMQ$Poller/POLLIN)
         (if (= -1 (.poll items TIMEOUT))
           (throw+ {:type ::INTERUPTER}))
         (if (true? (.pollin items 0))
           (let [^ZMsg msg (ZMsg/recvMsg @worker)]
             (if (nil? msg)
               (throw+ {:type ::INTERUPTER}))
             (if verbose
               (do
                 (info "I: received message from broker:")
                 (.dump msg *out*)))
             (reset! liveness HEARTBEAT_LIVENESS)
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
                                                   (reset! reply_to (.unwrap msg))
                                                   (.destroy command)
                                                   (deliver result msg))
                (frame-equals W_HEARTBEAT command) nil
                (frame-equals W_HEARTBEAT command) (do
                                                     (reconnect-to-broker this))
                :else (do 
                        (info "E: invalid input message: \n")
                        (.dump msg *out*))))))
         (if (= 0 (swap! liveness - 1))
           (do
             (if verbose
               (info "W: disconnected from broker - retrying\n"))
             (try
               (.sleep Thread RECONECT)
               (catch InterruptedException e
                 (.interrupt (.currentThread Thread))))
             (reconnect-to-broker this))))
       (if (and (not (realized? result)) 
                (> (System/currentTimeMillis ) @heartbeat_at))
         (do
           (send-to-broker this W_HEARTBEAT nil nil)
           (reset! heartbeat_at (+(System/currentTimeMillis ) HEARTBEAT)))))
      (if (realized? result)
        @result
        nil)))
  
  (destroy [this]
    #_(.terminate ctx))
  
  ) 

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
