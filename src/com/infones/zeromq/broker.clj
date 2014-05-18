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
            ZMQ$Socket]
           [com.infones.zeromq.mdp
            MajorDomo]))

(def ^:private ^:const ^String INTERNAL_SERVICE_PREFIX "mmi.")
(def ^:private ^:const ^Integer HEARTBEAT_LIVENESS 3)
(def ^:private ^:const ^Integer HEARTBEAT_INTERVAL 2500)
(def ^:private ^:const ^Integer HEARTBEAT_EXPIRY (* HEARTBEAT_INTERVAL HEARTBEAT_LIVENESS))

(defn- invalid-message [msg]
  (info "E: invalid message:\n")
  (.dump msg *out*))

(defrecord Service [^String id requests waiting])

(defn create-service [^String id]
  (->Service id (atom []) (atom [])))

(defrecord Worker [^String id ^ZFrame address ^Service service expiry]) ;service must be promise, expiry must be atom

(defn create-worker [^String id ^ZFrame address]
  (->Worker id address (promise ) (atom 0)))

(defrecord Broker [^ZMQ$Context ctx ^ZMQ$Socket socket heartbeat_at services workers waiting ^Boolean verbose] )

(defprotocol MajorDomoBroker
  (mediate [^Broker this])
  (destroy [^Broker this])
  (process-client [^Broker this ^ZFrame sender ^ZMsg msg])
  (process-worker [^Broker this ^ZFrame sender ^ZMsg msg])
  (delete-worker [^Broker this ^Worker worker ^Boolean disconnect])
  (require-worker [^Broker this ^ZFrame address])
  (require-service [^Broker this ^ZFrame service_frame])
  (bind [^Broker this ^String endpoint])
  (service-internal [^Broker this ^ZFrame service_frame, ^ZMsg msg])
  (send-heartbeats [^Broker this])
  (purge-workers [^Broker this])
  (worker-waiting [^Broker this ^Worker worker])
  (dispatch [^Broker this ^Service service ^ZMsg msg])
  (send-to-worker [^Broker this ^Worker worker ^MajorDomo command ^String option ^ZMsg msgp]))

(def majordomo-broker-impl
  {
   :mediate (fn [^Broker this]
              (while (not (.isInterrupted (Thread/currentThread)))
                (let [^ZMQ$Poller items (.poller (:ctx this) 1)]
                  (.register items (:socket this) ZMQ$Poller/POLLIN)
                  (if (= -1 (.poll items HEARTBEAT_INTERVAL))
                    (throw+ {:type ::INTERUPTER}))
                  (if (true? (.pollin items 0))
                    (let [^ZMsg msg (ZMsg/recvMsg (:socket this))]
                      (if (nil? msg)
                        (throw+ {:type ::INTERUPTER}))
                      (if (:verbose this)
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
  
  :destroy (fn [^Broker this]
             (doseq [^Worker worker (vals @(:workers this))]
               (delete-worker this worker true))
             (.term ctx))
  
  :process-client (fn [^Broker this ^ZFrame sender ^ZMsg msg]
                    (assert (> (.size msg) 1))
                    (if (:verbose this)
                      (do
                        (info "process client message" (.strhex sender))
                        (.dump msg *out*)))
                    (let [^ZFrame service_frame (.pop msg)]
                      (.wrap msg (.duplicate sender))
                      (if (.startsWith (.toString service_frame) INTERNAL_SERVICE_PREFIX)
                        (service-internal this service_frame msg)
                        (dispatch this (require-service this service_frame) msg))
                      (.destroy service_frame)))
  
  :process-worker (fn [^Broker this ^ZFrame sender ^ZMsg msg]
                    (assert (> (.size msg) 0))
                    (if (:verbose this)
                      (do
                        (info "process worker message at address" (.strhex sender))
                        (.dump msg *out*)))
                    (let [^ZFrame command (.pop msg)
                          ^boolean worker_ready (contains? @(:workers this) (.strhex sender))
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
                                                                   (.send msg (:socket this))
                                                                   (worker-waiting this worker))
                                                                 (delete-worker this worker true))
                        (true? (frame-equals W_HEARTBEAT command)) (if worker_ready
                                                                     (reset! (:expiry worker) (+ (System/currentTimeMillis) HEARTBEAT_EXPIRY))
                                                                     (delete-worker this worker true))
                        
                        (true? (frame-equals W_DISCONNECT command)) (delete-worker this worker false)
                        :else (invalid-message msg))
                      (.destroy msg)))
  
  :delete-worker (fn [^Broker this ^Worker worker ^Boolean disconnect]
                   (assert (not (nil? worker)))
                   (if (:verbose this)
                     (info "delete worker:" worker))
                   (if disconnect
                     (send-to-worker this worker W_DISCONNECT nil nil))
                   (if (realized? (:service worker))
                     (swap! (:waiting @(:service worker)) disj worker))
                   (swap! (:workers this) dissoc worker)
                   (.destroy (.address worker)))
  
  :require-worker (fn [^Broker this ^ZFrame address]
                    (assert (not (nil? address)))
                    (if (:verbose this)
                      (info "require worker at address" (.strhex address)))
                    (let [^String id (.strhex address)]
                      (if-let [^Worker worker (get @(:workers this) id)]
                        worker
                        (let [n_worker (create-worker id (.duplicate address))]
                          (swap! (:workers this) assoc id n_worker)
                          (if (:verbose this)
                            (info "I: registering new worker:" id))
                          n_worker))))
  
  :require-service (fn [^Broker this ^ZFrame service_frame]
                     (assert (not (nil? service_frame)))
                     (if (:verbose this)
                       (info "require service" (.toString service_frame)))
                     (let [^String id (.toString service_frame)]
                       (if-let [^Service service (get @(:services this) id)]
                         service
                         (let [n_service (create-service id)]
                           (swap! (:services this) assoc id n_service)
                           n_service))))
  
  :bind (fn [^Broker this ^String endpoint]
          (assert (not (nil? endpoint)))
          (.bind (:socket this) endpoint)
          (info "I: MDP broker/0.1.1 is active at " endpoint))
  
  :service-internal (fn [^Broker this ^ZFrame service_frame ^ZMsg msg]
                      (assert (not (nil? service_frame)))
                      (assert (not (nil? msg)))
                      (info "service internal" (.toString service_frame))
                      (let [^String return_code (if (= "mmi.service" (.toString service_frame))
                                                  (let [^String id (.toString (.peekLast msg))]
                                                    (if (contains? @(:services this) id)
                                                      "200"
                                                      "400"))
                                                  "501")
                            ^ZFrame client (.unwrap msg)]
                        (.reset (.peekLast msg) (.getBytes return_code))
                        (.addFirst msg (.duplicate service_frame))
                        (.addFirst msg (new-frame C_CLIENT))
                        (.wrap msg client)
                        (.send msg (:socket this))))
  
  :send-heartbeats(fn [^Broker this]
                    #_(info "send heartbeats")
                    (if (>= (System/currentTimeMillis ) @(:heartbeat_at this))
                      (do
                        (doseq [^Worker worker @(:waiting this)]
                          (send-to-worker this worker W_HEARTBEAT nil nil))
                        (reset! (:heartbeat_at this) (+ (System/currentTimeMillis ) HEARTBEAT_INTERVAL)))))
  
  :purge-workers (fn [^Broker this]
                   #_(info "purge workers")
                   (loop [ws @(:waiting this)
                          ^Worker w (first ws)]
                     (if (and
                           (not (nil? w))
                           (< @(:expiry w) (System/currentTimeMillis )))
                       (do
                         (info "I: deleting expired worker: " (:id w))
                         (delete-worker this w false)
                         (recur (next ws)
                                (second ws))))))
  
  :worker-waiting (fn [^Broker this ^Worker worker]
                    (assert (not (nil? worker)))
                    (swap! (:waiting this) conj worker)
                    (swap! (:waiting @(:service worker)) conj worker)
                    (reset! (:expiry worker) (+ (System/currentTimeMillis ) HEARTBEAT_EXPIRY))
                    (dispatch this @(:service worker) nil))
  
  :dispatch (fn [^Broker this ^Service service ^ZMsg msg]
              (assert (not (nil? service)))
              (if (not (nil? msg)) 
                (swap! (:requests service) conj msg))
              (purge-workers this)
              (while (and (not-empty @(:waiting service))
                          (not-empty @(:requests service)))
                (let [^ZMsg n_msg (first @(:requests service))
                      ^Worker worker (first @(:waiting service))]
                  (swap! (:requests service) next)
                  (swap! (:waiting service) next)
                  (swap! (:waiting this) next)
                  (send-to-worker this worker W_REQUEST nil n_msg)
                  (.destroy n_msg))))
  
  :send-to-worker (fn [^Broker this ^Worker worker ^MajorDomo command ^String option ^ZMsg msgp]
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
                      (if (:verbose this)
                        (do
                          (info "I: sending " command " to worker")
                          (.dump msg *out*)))
                      (.send msg (:socket this))))
   })

(extend Broker
  MajorDomoBroker
  majordomo-broker-impl)

(defn create-broker [verbose]
  (let [^ZMQ$Context ctx (ZMQ/context 1)
        ^ZMQ$Socket socket (.socket ctx ZMQ/ROUTER)]
    (->Broker ctx socket (atom 0) (atom {}) (atom {}) (atom []) verbose)))




