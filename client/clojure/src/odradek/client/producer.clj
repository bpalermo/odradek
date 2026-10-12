(ns odradek.client.producer
  "The produce half of the binding: the Java protocol core (ADR 1) driven over a
  clj-grpc stream. The core decides everything about the protocol; this owns
  the stream, the lock, the timers and the results handed to the application.
  It makes the choices the Java client's Producer makes. Not part of the public
  API; see odradek.client."
  (:require [odradek.client.records :as records]
            [odradek.client.stream :as stream])
  (:import [io.github.bpalermo.odradek.client.core ProduceCore]
           [io.github.bpalermo.odradek.client.v1 Close ProduceAction ProduceEvent ProduceSetup
            SendRejection StreamEnded StreamOpened]
           [io.github.bpalermo.odradek.kafka.v1 ProduceRecord ProduceResponse ProduceResult]
           [java.util ArrayList HashMap List UUID]
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit TimeoutException]))

(set! *warn-on-reflection* true)

(def ^:private close-timeout-s 60)

(def ^:private ^ProduceEvent stream-opened
  (-> (ProduceEvent/newBuilder) (.setStreamOpened (StreamOpened/getDefaultInstance)) (.build)))

(def ^:private ^ProduceEvent close-event
  (-> (ProduceEvent/newBuilder) (.setClose (Close/getDefaultInstance)) (.build)))

(defn- stream-ended
  ^ProduceEvent [^String code ^String message]
  (let [ended (StreamEnded/newBuilder)]
    (.setStatusCode ended code)
    (when message
      (.setMessage ended message))
    (-> (ProduceEvent/newBuilder) (.setStreamEnded ended) (.build))))

(defn- complete!
  [^CompletableFuture future ^ProduceResult result]
  (let [outcome (records/send-outcome result)]
    (if (instance? Throwable outcome)
      (.completeExceptionally future outcome)
      (.complete future outcome))))

(declare open-stream! apply!)

(defn- perform!
  "Carries out actions. Called with the lock held; results reach the
  application after it is released, through `after`."
  [{:keys [lock scheduler backoff requests fatal ^HashMap futures] :as producer}
   ^List actions ^ArrayList after]
  (doseq [^ProduceAction action actions]
    (case (.name (.getKindCase action))
      "TRANSMIT" (stream/transmit! @requests (.getTransmit action))
      "COMPLETED" (let [result (.getResult (.getCompleted action))]
                    (when-let [future (.remove futures (.getSeq result))]
                      (.add after #(complete! future result))))
      "OPEN_STREAM" (stream/schedule! scheduler
                                      (stream/next-delay-ms! backoff)
                                      #(locking lock (open-stream! producer)))
      "HALF_CLOSE" (stream/half-close! @requests)
      "FATAL" (vreset! fatal (str (.getStatusCode (.getFatal action))
                                  ": " (.getMessage (.getFatal action))))
      nil)))

(defn- apply!
  "Applies an event that came from the stream of `from-generation`, or from the
  application (-1)."
  [{:keys [lock ^ProduceCore core backoff requests generation closed
           ^HashMap futures ^CompletableFuture drained]
    :as producer}
   ^ProduceEvent event from-generation]
  (let [after (ArrayList.)]
    (locking lock
      ;; An event of a stream that has since been replaced is ignored.
      (when (or (neg? (long from-generation)) (= from-generation @generation))
        (when (.hasClose event)
          (vreset! closed true))
        (when (.hasStreamEnded event)
          (vreset! requests nil))
        (when (and (.hasReceived event) (.hasReady (.getReceived event)))
          (stream/reset-backoff! backoff))
        (perform! producer (.handle core event) after)
        (when (and @closed (.isEmpty futures))
          (.complete drained nil))
        (.notifyAll ^Object lock)))
    (doseq [f after]
      (f))))

(defn- open-stream!
  "Opens a stream and tells the core. Called with the lock held."
  [{:keys [channel ^ProduceCore core requests generation shut-down] :as producer}]
  (when-not @shut-down
    (let [mine (vswap! generation inc)]
      (vreset! requests
               (stream/open channel
                            :produce
                            (fn [^ProduceResponse response]
                              (apply! producer
                                      (-> (ProduceEvent/newBuilder) (.setReceived response) (.build))
                                      mine))
                            (fn [code message]
                              (apply! producer (stream-ended code message) mine))))
      (perform! producer (.handle core stream-opened) (ArrayList.)))))

(defn start
  "Starts a producer on a connection's channel and scheduler."
  [channel scheduler {:keys [max-block-ms max-buffered-records max-buffered-bytes contracts]}]
  (let [setup (doto (ProduceSetup/newBuilder)
                (.setSessionId (str (UUID/randomUUID)))
                (.setClient stream/client-info))
        _ (when (some-> max-buffered-records pos?)
            (.setMaxBufferedRecords setup (int max-buffered-records)))
        _ (when (some-> max-buffered-bytes pos?)
            (.setMaxBufferedBytes setup (long max-buffered-bytes)))
        producer {:lock (Object.)
                  :channel channel
                  :scheduler scheduler
                  :core (ProduceCore. (.build setup))
                  :contracts contracts
                  :max-block-ms (long (or max-block-ms 60000))
                  :backoff (stream/backoff)
                  ;; seq -> the future of the record it was assigned to.
                  :futures (HashMap.)
                  :drained (CompletableFuture.)
                  :requests (volatile! nil)
                  :generation (volatile! 0)
                  :closed (volatile! false)
                  :shut-down (volatile! false)
                  :fatal (volatile! nil)}]
    (locking (:lock producer)
      (open-stream! producer))
    producer))

(defn- wait-for-room!
  "Waits on the lock, which the caller holds, for at most `nanos`."
  [^Object lock ^long nanos]
  (try
    (.timedWait TimeUnit/NANOSECONDS lock nanos)
    (catch InterruptedException _
      (.interrupt (Thread/currentThread))
      (throw (ex-info "interrupted while waiting for room in the window"
                      {:odradek/error :buffer-full})))))

(defn send!
  "Sends a record map; see odradek.client/send!."
  ^CompletableFuture
  [{:keys [lock ^ProduceCore core contracts max-block-ms fatal ^HashMap futures] :as producer}
   record]
  (let [^ProduceRecord message (records/produce-record contracts record)
        event (-> (ProduceEvent/newBuilder) (.setSend message) (.build))
        deadline (+ (System/nanoTime) (.toNanos TimeUnit/MILLISECONDS (long max-block-ms)))
        future (CompletableFuture.)
        after (ArrayList.)]
    (try
      (locking lock
        (loop []
          (let [actions (.handle core event)
                ^ProduceAction answer (.get actions 0)]
            (if (.hasAccepted answer)
              (do
                (.put futures (.getSeq (.getAccepted answer)) future)
                (perform! producer actions after)
                future)
              (let [reason (.getReason (.getRejected answer))]
                (when (= reason SendRejection/SEND_REJECTION_TOO_LARGE)
                  (throw (ex-info "the record is larger than the whole in-flight window"
                                  {:odradek/error :record-too-large})))
                (when (= reason SendRejection/SEND_REJECTION_CLOSED)
                  (throw (ex-info (if-let [why @fatal]
                                    (str "the server refused the producer: " why)
                                    "the producer is closed")
                                  {:odradek/error :closed})))
                (let [remaining (- deadline (System/nanoTime))]
                  (when-not (pos? remaining)
                    (throw (ex-info (str "the in-flight window stayed full for "
                                         max-block-ms " ms")
                                    {:odradek/error :buffer-full})))
                  (wait-for-room! lock remaining))
                (recur))))))
      (finally
        (doseq [f after]
          (f))))))

(defn close!
  "Stops taking records, waits up to a minute for the results of those already
  sent, and ends the stream. Records still unanswered then stay with the
  server; their futures do not complete."
  [{:keys [lock requests generation shut-down ^CompletableFuture drained] :as producer}]
  (apply! producer close-event -1)
  (try
    (.get drained (long close-timeout-s) TimeUnit/SECONDS)
    (catch InterruptedException _
      (.interrupt (Thread/currentThread)))
    (catch ExecutionException _ nil)
    (catch TimeoutException _ nil))
  (locking lock
    (vreset! shut-down true)
    (vswap! generation inc)
    (stream/cancel! @requests "producer closed")
    (vreset! requests nil))
  nil)
