(ns odradek.client.stream
  "What the producer and the consumer share as bindings of the protocol core:
  opening one of the two streams with clj-grpc, the delay between attempts, and
  what the client says about itself. Not part of the public API."
  (:require [clj-grpc.client :as grpc]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [odradek.kafka.v1.odradek :as api])
  (:import [io.github.bpalermo.odradek.kafka.v1 ClientInfo]
           [io.grpc Status]
           [java.util.concurrent RejectedExecutionException ScheduledExecutorService
            ThreadLocalRandom TimeUnit]))

(set! *warn-on-reflection* true)

(def version
  "The version of this library, from the file the Clojars pom is made from."
  (:version (edn/read-string (slurp (io/resource "odradek/client/version.edn")))))

(def ^ClientInfo client-info
  "What both handshakes carry about this library."
  (api/ClientInfo->proto {:language "clojure" :version version}))

;; ---------------------------------------------------------------------------
;; Backoff: doubling from 100 ms to 5 s, with jitter. The same as the Java client.

(def ^:private initial-ms 100)
(def ^:private max-ms 5000)

(defn backoff
  "A new backoff. Not thread-safe: its owner calls it under its lock."
  []
  (long-array [initial-ms]))

(defn next-delay-ms!
  "The delay before the next attempt to open a stream, in milliseconds."
  ^long [^longs backoff]
  (let [next-ms (aget backoff 0)
        half (quot next-ms 2)
        delay (+ half (.nextLong (ThreadLocalRandom/current) (inc half)))]
    (aset backoff 0 (long (min max-ms (* 2 next-ms))))
    delay))

(defn reset-backoff!
  "Called when a stream got as far as its handshake reply."
  [^longs backoff]
  (aset backoff 0 (long initial-ms)))

(defn schedule!
  "Runs f after delay-ms. A connection that was closed in the meantime has no
  scheduler any more, and then there is nothing left to run f for."
  [^ScheduledExecutorService scheduler ^long delay-ms f]
  (try
    (.schedule scheduler ^Runnable f delay-ms TimeUnit/MILLISECONDS)
    (catch RejectedExecutionException _ nil)))

;; ---------------------------------------------------------------------------
;; Streams

(defn open
  "Opens the :produce or the :consume stream on channel and returns its request
  side, {:send! :close! :error!}. on-message gets each message from the server.
  on-end is called once when the stream is gone, with the name of its gRPC
  status code (\"OK\" when the server completed it) and the description or nil.

  Both callbacks run on the channel's executor, one at a time per stream.
  :send! is not thread-safe; callers serialize it."
  [channel method on-message on-end]
  (grpc/invoke channel
               (get api/odradekservice-methods method)
               {:on-next on-message
                :on-error (fn [t]
                            (let [status (Status/fromThrowable t)]
                              (on-end (.name (.getCode status)) (.getDescription status))))
                :on-complete (fn [] (on-end "OK" nil))}))

(defn transmit!
  [stream message]
  (when stream
    ((:send! stream) message)))

(defn half-close!
  [stream]
  (when stream
    ((:close! stream))))

(defn cancel!
  "Cancels a stream this side no longer wants."
  [stream ^String why]
  (when stream
    (try
      ((:error! stream) (.asRuntimeException (.withDescription Status/CANCELLED why)))
      (catch RuntimeException _
        ;; The stream was already complete.
        nil))))
