(ns odradek.client.consumer
  "The consume half of the binding: the Java protocol core (ADR 1) driven over a
  clj-grpc stream. The core decides credits, acknowledgements and the revoke
  handshake; this owns the stream, the lock, the timers and the one thread the
  application's handler and callbacks run on. It makes the choices the Java
  client's Consumer makes. Not part of the public API; see odradek.client."
  (:require [clojure.string :as str]
            [odradek.client.records :as records]
            [odradek.client.stream :as stream])
  (:import [io.github.bpalermo.odradek.client.core ConsumeCore]
           [io.github.bpalermo.odradek.client.v1 Close ConsumeAction ConsumeEvent ConsumeSetup
            StreamEnded StreamOpened]
           [io.github.bpalermo.odradek.kafka.v1 Ack ConsumeResponse ConsumedRecord
            PartitionAssignment RecordBatch RevokeDone Revoked TopicPartition]
           [java.lang System$Logger System$Logger$Level]
           [java.util List Map Set]
           [java.util.concurrent CompletableFuture ConcurrentHashMap ExecutionException
            LinkedBlockingDeque TimeUnit TimeoutException]))

(set! *warn-on-reflection* true)

(def ^:private close-timeout-s 30)

(def ^:private ^System$Logger logger (System/getLogger "odradek.client.consumer"))

(def ^:private ^ConsumeEvent stream-opened
  (-> (ConsumeEvent/newBuilder) (.setStreamOpened (StreamOpened/getDefaultInstance)) (.build)))

(def ^:private ^ConsumeEvent close-event
  (-> (ConsumeEvent/newBuilder) (.setClose (Close/getDefaultInstance)) (.build)))

(defn- stream-ended
  ^ConsumeEvent [^String code ^String message]
  (let [ended (StreamEnded/newBuilder)]
    (.setStatusCode ended code)
    (when message
      (.setMessage ended message))
    (-> (ConsumeEvent/newBuilder) (.setStreamEnded ended) (.build))))

(defn- partitions
  [topic-partitions]
  (mapv (fn [^TopicPartition p] (records/partition-of (.getTopic p) (.getPartition p)))
        topic-partitions))

(declare open-stream! close!)

(defn- perform!
  "Carries out actions. Called with the lock held; application code runs later,
  on the handler thread, from the work queue."
  [{:keys [lock scheduler backoff requests stopped
           ^LinkedBlockingDeque work ^Map live ^Set leaving]
    :as consumer}
   ^List actions]
  (doseq [^ConsumeAction action actions]
    (case (.name (.getKindCase action))
      "TRANSMIT" (stream/transmit! @requests (.getTransmit action))
      "ASSIGNED" (let [assigned (.getPartitionsList (.getAssigned action))]
                   (doseq [^PartitionAssignment p assigned]
                     (let [partition (records/partition-of (.getTopic p) (.getPartition p))]
                       (.remove leaving partition)
                       (.put live partition (.getAssignmentEpoch p))))
                   (.add work [:assigned
                               (mapv (fn [^PartitionAssignment p]
                                       (records/partition-of (.getTopic p) (.getPartition p)))
                                     assigned)]))
      "DELIVER" (.add work [:records (.getDeliver action)])
      "REVOKING" (let [revoked (.getRevoking action)]
                   (.addAll leaving (partitions (.getPartitionsList revoked)))
                   ;; Ahead of the records already queued: the server is waiting
                   ;; for the answer.
                   (.addFirst work [:revoking revoked]))
      "LOST" (let [lost (partitions (.getPartitionsList (.getLost action)))]
               (doseq [partition lost]
                 (.remove live partition))
               (.addFirst work [:lost lost]))
      "OPEN_STREAM" (stream/schedule! scheduler
                                      (stream/next-delay-ms! backoff)
                                      #(locking lock (open-stream! consumer)))
      "HALF_CLOSE" (stream/half-close! @requests)
      "FATAL" (let [fatal (.getFatal action)]
                (vreset! stopped true)
                (.addFirst work [:failed
                                 (ex-info (str (.getStatusCode fatal) ": " (.getMessage fatal))
                                          {:odradek/error :consumer-failed
                                           :status-code (.getStatusCode fatal)
                                           :message (.getMessage fatal)})]))
      nil)))

(defn- apply!
  "Applies an event that came from the stream of `from-generation`, or from the
  application (-1)."
  [{:keys [lock ^ConsumeCore core backoff requests generation stopped shut-down
           ^CompletableFuture ended]
    :as consumer}
   ^ConsumeEvent event from-generation]
  (locking lock
    ;; An event of a stream that has since been replaced is ignored.
    (when (or (neg? (long from-generation)) (= from-generation @generation))
      (when (.hasStreamEnded event)
        (vreset! requests nil))
      (when (and (.hasReceived event) (.hasSubscribed (.getReceived event)))
        (stream/reset-backoff! backoff))
      (perform! consumer (.handle core event))
      (when (and (.hasStreamEnded event) (nil? @requests) (or @stopped @shut-down))
        (.complete ended nil)))))

(defn- open-stream!
  "Opens a stream and tells the core. Called with the lock held."
  [{:keys [channel ^ConsumeCore core requests generation stopped shut-down] :as consumer}]
  (when-not (or @shut-down @stopped)
    (let [mine (vswap! generation inc)]
      (vreset! requests
               (stream/open channel
                            :consume
                            (fn [^ConsumeResponse response]
                              (apply! consumer
                                      (-> (ConsumeEvent/newBuilder) (.setReceived response) (.build))
                                      mine))
                            (fn [code message]
                              (apply! consumer (stream-ended code message) mine))))
      (perform! consumer (.handle core stream-opened)))))

(defn- ack
  ^ConsumeEvent [^RecordBatch batch ^ConsumedRecord record]
  (-> (ConsumeEvent/newBuilder)
      (.setAck (-> (Ack/newBuilder)
                   (.setTopic (.getTopic batch))
                   (.setPartition (.getPartition batch))
                   (.setAssignmentEpoch (.getAssignmentEpoch batch))
                   (.setFirstOffset (.getOffset record))
                   (.setLastOffset (.getOffset record))))
      (.build)))

(defn- close-in-background!
  "Closes the consumer from a thread of its own: closing waits for the handler
  thread and for the server, and neither the handler thread nor the timer
  thread may wait for that."
  [consumer]
  (doto (Thread. ^Runnable #(close! consumer) "odradek-consumer-close")
    (.setDaemon true)
    (.start)))

(defn- handle!
  "Hands the records of a batch to the handler, one at a time, and acks each
  when the handler returns. Returns false when the handler threw: the consumer
  has failed and the handler thread ends."
  [{:keys [contracts handler on-failed stopped ^LinkedBlockingDeque work ^Map live ^Set leaving]
    :as consumer}
   ^RecordBatch batch]
  (let [partition (records/partition-of (.getTopic batch) (.getPartition batch))
        epoch (.getAssignmentEpoch batch)]
    (loop [remaining (seq (.getRecordsList batch))]
      (let [owned (.get live partition)]
        (if (or (nil? remaining)
                @stopped
                (nil? owned)
                (not= (long owned) epoch)
                (.contains leaving partition))
          ;; Done, or not ours to process any more. What is left is unacked, so
          ;; it is delivered again, here or elsewhere.
          true
          (let [^ConsumedRecord record (first remaining)
                failure (try
                          (handler (records/received contracts batch record))
                          nil
                          (catch Exception e e))]
            (if failure
              (do
                ;; Not acked, and nothing after it is processed: the order
                ;; within the partition holds for whoever reads it next.
                (vreset! stopped true)
                (.clear work)
                (try
                  (on-failed failure)
                  (finally
                    (close-in-background! consumer)))
                false)
              (do
                (apply! consumer (ack batch record) -1)
                (recur (next remaining))))))))))

(defn- revoke!
  [{:keys [on-revoked ^Map live ^Set leaving] :as consumer} ^Revoked revoked]
  (let [revoking (partitions (.getPartitionsList revoked))]
    (try
      (on-revoked revoking)
      (finally
        (doseq [partition revoking]
          (.remove live partition)
          (.remove leaving partition))
        (apply! consumer
                (-> (ConsumeEvent/newBuilder)
                    (.setRevokeDone (-> (RevokeDone/newBuilder)
                                        (.setRevokeId (.getRevokeId revoked))))
                    (.build))
                -1)))))

(defn- run
  "The handler thread: everything the application supplied runs here, in the
  order the work was queued."
  [{:keys [on-assigned on-lost on-failed ^LinkedBlockingDeque work] :as consumer}]
  (loop []
    (when-let [[kind payload] (try
                                (.take work)
                                (catch InterruptedException _ nil))]
      (when (try
              (case kind
                :records (handle! consumer payload)
                :assigned (do (on-assigned payload) true)
                :revoking (do (revoke! consumer payload) true)
                :lost (do (on-lost payload) true)
                :failed (do (on-failed payload) false)
                :stop false)
              (catch RuntimeException e
                (.log logger System$Logger$Level/ERROR "a consumer callback threw" e)
                (not (contains? #{:records :failed :stop} kind))))
        (recur)))))

(defn start
  "Starts a consumer on a connection's channel and scheduler."
  [channel scheduler
   {:keys [topics max-processing-ms max-unacked-records contracts
           on-assigned on-revoked on-lost on-failed]}
   handler]
  (when-not (and (seq topics) (every? string? topics))
    (throw (ex-info "a consumer needs :topics, a collection of topic names"
                    {:odradek/error :invalid-options})))
  (when-not (ifn? handler)
    (throw (ex-info "a consumer needs a handler fn" {:odradek/error :invalid-options})))
  (let [setup (doto (ConsumeSetup/newBuilder)
                (.addAllTopics ^Iterable (vec topics))
                (.setClient stream/client-info))
        _ (when (some-> max-processing-ms pos?)
            (.setMaxProcessingMs setup (int max-processing-ms)))
        _ (when (some-> max-unacked-records pos?)
            (.setMaxUnackedRecords setup (int max-unacked-records)))
        ignore (fn [_] nil)
        consumer {:lock (Object.)
                  :channel channel
                  :scheduler scheduler
                  :core (ConsumeCore. (.build setup))
                  :contracts contracts
                  :handler handler
                  :on-assigned (or on-assigned ignore)
                  :on-revoked (or on-revoked ignore)
                  :on-lost (or on-lost ignore)
                  :on-failed (or on-failed ignore)
                  :backoff (stream/backoff)
                  ;; Something for the handler thread to do, in order: [kind payload].
                  :work (LinkedBlockingDeque.)
                  ;; Partitions whose records may be handled -> the epoch they
                  ;; are owned under.
                  :live (ConcurrentHashMap.)
                  ;; Partitions being taken away: their records still queued
                  ;; are skipped.
                  :leaving (ConcurrentHashMap/newKeySet)
                  :ended (CompletableFuture.)
                  :requests (volatile! nil)
                  :generation (volatile! 0)
                  :stopped (volatile! false)
                  :shut-down (volatile! false)
                  :thread (volatile! nil)}
        thread (doto (Thread. ^Runnable #(run consumer)
                              (str "odradek-consumer-" (str/join "," topics)))
                 (.setDaemon true))]
    (vreset! (:thread consumer) thread)
    (.start thread)
    (locking (:lock consumer)
      (open-stream! consumer))
    consumer))

(defn close!
  "Stops handling records after the one in progress, lets the server commit
  what was processed, and leaves the consumer group. Waits up to 30 seconds for
  each of the two."
  [{:keys [lock requests generation stopped shut-down thread
           ^LinkedBlockingDeque work ^CompletableFuture ended]
    :as consumer}]
  (vreset! stopped true)
  (.addFirst work [:stop nil])
  (when-not (identical? (Thread/currentThread) @thread)
    (try
      (.join ^Thread @thread (.toMillis TimeUnit/SECONDS (long close-timeout-s)))
      (catch InterruptedException _
        (.interrupt (Thread/currentThread)))))
  (apply! consumer close-event -1)
  (locking lock
    ;; Without a stream there is no end of one to wait for.
    (when (nil? @requests)
      (.complete ended nil)))
  (try
    (.get ended (long close-timeout-s) TimeUnit/SECONDS)
    (catch InterruptedException _
      (.interrupt (Thread/currentThread)))
    (catch ExecutionException _ nil)
    (catch TimeoutException _ nil))
  (locking lock
    (vreset! shut-down true)
    (vswap! generation inc)
    (stream/cancel! @requests "consumer closed")
    (vreset! requests nil))
  nil)
