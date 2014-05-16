(ns com.infones.zeromq.majordomo
  (:require [com.infones.zeromq 
             [broker :as broker]
             [client :as client]
             [worker :as worker]])
  (:import [org.jeromq 
            ZMsg]))

(defn worker-life []
  (let [worker_session (worker/create-worker "tcp://localhost:5555"
                                             "echo"
                                             false)]
    (loop [^ZMsg reply nil]
     (if-not (.isInterrupted (Thread/currentThread))
       (if-let [^ZMsg request (worker/receive worker_session reply)]
         (recur request))))
    (worker/destroy worker_session)))

(defn client-life []
  (let [client_session (client/create-client "tcp://localhost:5555" true)]
    (dotimes [i 100]
      (let [^ZMsg request (ZMsg. )]
        (.addString request (format "Hellou %d" i))
        (client/send-request client_session "echo" request)))
    (dotimes [i 100]
      (let [^ZMsg reply (client/recv client_session)]
        (if-not (nil? reply)
          (comment
            (println "REPLY")
            (.dump reply *out*)
            (.destroy reply))
          (println "ERROR"))))
    (client/destroy client_session)))

(defn broker-life []
  (let [broker (broker/create-broker false)]
    (broker/bind broker "tcp://*:5555")
    (broker/mediate broker)))

(defn run []
  (future-call broker-life)
  (future-call worker-life))

#_(def b (future (broker-life )))
#_(def w (future (worker-life )))

