(ns com.infones.zeromq.broker
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

(def ^:private ^:const ^String INTERNAL_SERVICE_PREFIX "mmi.")
(def ^:private ^:const ^Integer HEARTBEAT_LIVENESS 3)
(def ^:private ^:const ^Integer HEARTBEAT_INTERVAL 2500)
(def ^:private ^:const ^Integer HEARTBEAT_EXPIRY (* HEARTBEAT_INTERVAL HEARTBEAT_LIVENESS))

(defn- invalid-message [msg]
  (info "E: invalid message:\n")
  (.dump msg *out*))

(defrecord Service [^String id requests waiting]) ;waiting must be ref vec

(defn create-service [^String id]
  (->Service id (ref []) (ref [])))

(defrecord Worker [^String id ^ZFrame address ^Service service expiry]) ;service must be promise, expiry must be atom

(defn create-worker [^String id ^ZFrame address]
  (->Worker id address (promise ) (atom 0)))

(defprotocol MajorDomoBroker
  (mediate [this])
  (destroy [this])
  (process-client [this ^ZFrame sender ^ZMsg msg])
  (process-worker [this ^ZFrame sender ^ZMsg msg])
  (delete-worker [this ^Worker worker ^Boolean disconnect])
  (require-worker [this ^ZFrame address])
  (require-service [this ^ZFrame service_frame])
  (bind [this ^String endpoint])
  (service-internal [this ^ZFrame service_frame, ^ZMsg msg])
  (send-heartbeats [this])
  (purge-workers [this])
  (worker-waiting [this ^Worker worker])
  (dispatch [this ^Service service ^ZMsg msg])
  (send-to-worker [this ^Worker worker command ^String option ^ZMsg msgp]))

(defrecord Broker [^ZMQ$Context ctx ^ZMQ$Socket socket heartbeat_at services workers waiting ^Boolean verbose] ;workers/services must be ref map, waiting must be ref vec, heartbeat_at is atom
  MajorDomoBroker
  (mediate [this]
    (while (not (.isInterrupted (Thread/currentThread)))
      (let [items (.poller ctx 1)]
        (.register items socket ZMQ$Poller/POLLIN)
        (if (= -1 (.poll items HEARTBEAT_INTERVAL))
          (throw+ {:type ::INTERUPTER}))
        (if (true? (.pollin items 0))
          (let [^ZMsg msg (ZMsg/recvMsg socket)]
            (if (nil? msg)
              (throw+ {:type ::INTERUPTER}))
            (if verbose
              (do 
                (info "I: received message:\n")
                (.dump msg *out*)))
            (let [^ZFrame sender (.pop msg)
                  ^ZFrame empty_ (.pop msg)
                  ^ZFrame header (.pop msg)]
              (cond 
                (true? (frame-equals C_CLIENT header)) (process-client this sender msg)
                (true? (frame-equals W_WORKER header)) (process-worker this sender msg)
                :else (do 
                        (invalid-message msg)
                        (.destroy msg)))
              (.destroy sender)
              (.destroy empty_)
              (.destroy header))))
        (purge-workers this)
        (send-heartbeats this)))
    (destroy this))
  
  (destroy [this]
    (doseq [worker (vals @workers)]
      (delete-worker this worker true))
    (.destroy ctx))
  
  (process-client [this sender msg]
    (assert (> (.size msg) 1))
    (if verbose
      (do
        (info "process client message" (.strhex sender))
        (.dump msg *out*)))
    (let [^ZFrame service_frame (.pop msg)]
      (.wrap msg (.duplicate sender))
      (if (.startsWith (.toString service_frame) INTERNAL_SERVICE_PREFIX)
        (service-internal this service_frame msg)
        (dispatch this (require-service this service_frame) msg))
      (.destroy service_frame)))
  
  (process-worker [this sender msg]
    (assert (> (.size msg) 0))
    (if verbose
      (do
        (info "process worker message at address" (.strhex sender))
        (.dump msg *out*)))
    (let [^ZFrame command (.pop msg)
          ^boolean worker_ready (contains? @workers (.strhex sender))
          ^Worker worker (require-worker this sender)]
      (cond
        (true? (frame-equals W_READY command)) (if (or worker_ready
                                                       (.startsWith (.toString sender) INTERNAL_SERVICE_PREFIX))
                                                 (delete-worker this worker true)
                                                 (let [^ZFrame service_frame (.pop msg)]
                                                   (deliver (:service worker) (require-service this service_frame))
                                                   (worker-waiting this worker)
                                                   (.destroy service_frame)))
        (true? (frame-equals W_REPLY command)) (if worker_ready
                                                 (let [^ZFrame client (.unwrap msg)]
                                                   (.addFirst msg (:id @(:service worker)))
                                                   (.addFirst msg (new-frame C_CLIENT))
                                                   (.wrap msg client)
                                                   (.send msg socket)
                                                   (worker-waiting this worker))
                                                 (delete-worker this worker true))
        (true? (frame-equals W_HEARTBEAT command)) (if worker_ready
                                                    (reset! (:expiry worker) (+ (System/currentTimeMillis) HEARTBEAT_EXPIRY))
                                                    (delete-worker this worker true))
        
        (true? (frame-equals W_DISCONNECT command)) (delete-worker this worker false)
        :else (invalid-message msg))
      (.destroy msg)))
  
  (delete-worker [this worker disconnect]
    (assert (not (nil? worker)))
    (if verbose
      (info "delete worker:" worker))
    (if disconnect
      (send-to-worker this worker W_DISCONNECT nil nil))
    (if (realized? (:service worker))
      (swap! (:waiting @(:service worker)) disj worker))
    (swap! workers dissoc worker)
    (.destroy (.address worker)))
  
  (require-worker [this address]
    (assert (not (nil? address)))
    (if verbose
      (info "require worker at address" (.strhex address)))
    (let [^String id (.strhex address)]
      (if-let [^Worker worker (get @workers id)]
        worker
        (let [n_worker (create-worker id (.duplicate address))]
          (swap! workers assoc id n_worker)
          (if verbose
            (info "I: registering new worker:" id))
          n_worker))))
  
  (require-service [this service_frame]
    (assert (not (nil? service_frame)))
    (if verbose
      (info "require service" (.toString service_frame)))
    (let [^String id (.toString service_frame)]
      (if-let [^Service service (get @services id)]
        service
        (let [n_service (create-service id)]
          (swap! services assoc id n_service)
          n_service))))
  
  (bind [this endpoint]
    (assert (not (nil? endpoint)))
    (.bind socket endpoint)
    (info "I: MDP broker/0.1.1 is active at " endpoint))
  
  (service-internal [this service_frame msg]
    (assert (not (nil? service_frame)))
    (assert (not (nil? msg)))
    (info "service internal" (.toString service_frame))
    (let [return_code (if (= "mmi.service" (.toString service_frame))
                        (let [^String id (.toString (.peekLast msg))]
                          (if (contains? @services id)
                            "200"
                            "400"))
                        "501")
          ^ZFrame client (.unwrap msg)]
      (.reset (.peekLast msg) (.getBytes return_code))
      (.addFirst msg (.duplicate service_frame))
      (.addFirst msg (new-frame C_CLIENT))
      (.wrap msg client)
      (.send msg socket)))
  
  (send-heartbeats [this]
    #_(info "send heartbeats")
    (if (>= (System/currentTimeMillis ) @heartbeat_at)
      (do
        (doseq [^Worker worker @waiting]
          (send-to-worker this worker W_HEARTBEAT nil nil))
        (reset! heartbeat_at (+ (System/currentTimeMillis ) HEARTBEAT_INTERVAL)))))
  
  (purge-workers [this]
    #_(info "purge workers")
    (loop [ws @waiting
           w (first ws)]
      (if (and
            (not (nil? w))
            (< @(:expiry w) (System/currentTimeMillis )))
        (do
          (info "I: deleting expired worker: " (:id w))
          (delete-worker this w false)
          (recur (next ws)
                 (second ws))))))
  
  (worker-waiting [this worker]
    (assert (not (nil? worker)))
    (dosync
      (alter waiting conj worker)
      (alter (:waiting @(:service worker)) conj worker))
    (reset! (:expiry worker) (+ (System/currentTimeMillis ) HEARTBEAT_EXPIRY))
    (dispatch this @(:service worker) nil))
  
  (dispatch [this service msg]
    (assert (not (nil? service)))
    (if (not (nil? msg)) 
      (dosync
       (alter (:requests service) conj msg)))
    (purge-workers this)
    (while (and (not-empty @(:waiting service))
                (not-empty @(:requests service)))
      (let [^ZMsg n_msg (first @(:requests service))
            ^Worker worker (first @(:waiting service))]
        (dosync
          (alter (:requests service) next)
          (alter (:waiting service) next)
          (alter waiting next))
        (send-to-worker this worker W_REQUEST nil n_msg)
        (.destroy n_msg))))
  
  (send-to-worker [this worker command option msgp]
    (assert (not (nil? worker)))
    (assert (not (nil? command)))
    (let [^ZMsg msg (if (nil? msgp)
                      (ZMsg. )
                      (.duplicate msgp))]
      (if (not (nil? option))
        (.addFirst msg (ZFrame. option)))
      (.addFirst msg (new-frame command))
      (.addFirst msg (new-frame W_WORKER))
      (.wrap msg (.duplicate (:address worker)))
      (if verbose
        (do
          (info "I: sending " command " to worker")
          (.dump msg *out*)))
      (.send msg socket))))

(defn create-broker [verbose]
  (let [^ZMQ$Context ctx (ZMQ/context 1)
        ^ZMQ$Socket socket (.socket ctx ZMQ/ROUTER)]
    (->Broker ctx socket (atom 0) (atom {}) (atom {}) (ref []) verbose)))




