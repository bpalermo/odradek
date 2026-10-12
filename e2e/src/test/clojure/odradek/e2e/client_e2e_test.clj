(ns odradek.e2e.client-e2e-test
  "The Clojure client end to end: its public API against the packaged server,
  started as a process, and two real clusters. The same list of cases as
  ClientE2eTest, which does this for the Java client."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is use-fixtures]]
            [odradek.client :as odradek])
  (:import [java.io File]
           [java.lang ProcessBuilder$Redirect]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util UUID]
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit]
           [org.apache.kafka.clients.admin Admin NewTopic OffsetSpec]
           [org.apache.kafka.common TopicPartition]
           [org.apache.kafka.common.errors RetriableException]))

(def ^:private timeout-ms 60000)
(def ^:private partitions 4)

(defn- env
  [variable]
  (let [value (System/getenv variable)]
    (when (str/blank? value)
      (throw (ex-info (str variable " is not set; see e2e/BUILD.bazel") {:variable variable})))
    (str/trim value)))

(def ^:private kafka-a (delay (env "ODRADEK_E2E_KAFKA_A")))
(def ^:private kafka-b (delay (env "ODRADEK_E2E_KAFKA_B")))

;; Set per test by the fixture.
(def ^:private ^:dynamic *topic* nil)
(def ^:private ^:dynamic *unconfigured* nil)
(def ^:private ^:dynamic *server* nil)
(def ^:private ^:dynamic *conn* nil)

;; ---------------------------------------------------------------------------
;; Kafka

(defn- admin
  ^Admin [bootstrap]
  (Admin/create ^java.util.Map {"bootstrap.servers" bootstrap}))

(defn- create-topic!
  "Creates a topic and waits until every partition's leader answers. Producing in
  the gap between the two can wedge Kafka's idempotent producer until its
  delivery timeout; see ServerHarness.createTopic in the Java suite."
  [bootstrap topic]
  (with-open [^Admin admin (admin bootstrap)]
    (-> (.createTopics admin [(NewTopic. ^String topic (int partitions) (short 1))])
        (.all)
        (.get))
    (let [ends (into {}
                     (map (fn [p] [(TopicPartition. topic (int p)) (OffsetSpec/latest)]))
                     (range partitions))
          deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
      (loop []
        (let [answered? (try
                          (-> (.listOffsets admin ^java.util.Map ends)
                              (.all)
                              (.get 30 TimeUnit/SECONDS))
                          true
                          (catch ExecutionException e
                            (if (and (instance? RetriableException (.getCause e))
                                     (< (System/nanoTime) deadline))
                              false
                              (throw e))))]
          (when-not answered?
            (Thread/sleep 50)
            (recur)))))))

(defn- delete-topic!
  [bootstrap topic]
  (with-open [admin (admin bootstrap)]
    (-> (.deleteTopics admin ^java.util.Collection (vector topic))
        (.all)
        (.get))))

;; ---------------------------------------------------------------------------
;; The server: the executable ODRADEK_E2E_SERVER names, given --config, the way
;; a pod runs it.

(defn- start-process!
  [{:keys [^File config ^File socket]}]
  (let [executable (.getAbsolutePath (io/file (env "ODRADEK_E2E_SERVER")))
        process (-> (ProcessBuilder. ^java.util.List [executable "--config" (str config)])
                    (.redirectOutput ProcessBuilder$Redirect/INHERIT)
                    (.redirectError ProcessBuilder$Redirect/INHERIT)
                    (.start))
        deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (.exists socket) process
        (not (.isAlive process))
        (throw (ex-info (str executable " exited before listening")
                        {:status (.exitValue process)}))
        (> (System/nanoTime) deadline)
        (do (.destroyForcibly process)
            (throw (ex-info (str executable " did not listen on " socket) {})))
        :else (do (Thread/sleep 50) (recur))))))

(defn- stop-process!
  "Stops the server with SIGTERM."
  [{:keys [process ^File socket]}]
  (let [^Process running @process]
    (.destroy running)
    (when-not (.waitFor running 60 TimeUnit/SECONDS)
      (.destroyForcibly running)
      (throw (ex-info "the server did not stop within 60s of SIGTERM" {})))
    (io/delete-file socket true)))

(defn- start-server!
  [yaml]
  (let [dir (.toFile (Files/createTempDirectory (Path/of "/tmp" (make-array String 0))
                                                "odradek-e2e-clj"
                                                (make-array FileAttribute 0)))
        server {:dir dir
                :config (io/file dir "odradek.yaml")
                :socket (io/file dir "kafka.sock")
                :process (atom nil)}]
    (spit (:config server) (str/replace yaml "SOCKET" (str (:socket server))))
    (reset! (:process server) (start-process! server))
    server))

(defn- restart-server!
  "Stops the server and starts a new one with the same configuration and
  socket: a new server epoch, with no memory of produce sessions."
  [server]
  (stop-process! server)
  (reset! (:process server) (start-process! server)))

(defn- stop-server!
  [server]
  (try
    (stop-process! server)
    (finally
      (doseq [file (reverse (file-seq (:dir server)))]
        (io/delete-file file true)))))

(defn- config
  [id topic]
  (str/join
   "\n"
   ["listener:"
    "  socketPath: SOCKET"
    "clusters:"
    "  a:"
    "    properties:"
    (str "      bootstrap.servers: " @kafka-a)
    "    consumerProperties:"
    "      session.timeout.ms: 10000"
    "      heartbeat.interval.ms: 1000"
    "  b:"
    "    properties:"
    (str "      bootstrap.servers: " @kafka-b)
    "consume:"
    "  commitInterval: 0.2s"
    "  revokeDeadline: 5s"
    "producers:"
    (str "  " topic ":")
    "    clusters: [{cluster: a}, {cluster: b}]"
    "consumers:"
    (str "  " topic ":")
    "    cluster: a"
    (str "    groupId: clj-client-" id)
    ""]))

(defn- with-server
  [f]
  (let [id (str (UUID/randomUUID))
        topic (str "odradek-e2e-clj-client-" id)]
    (create-topic! @kafka-a topic)
    (create-topic! @kafka-b topic)
    (try
      (let [server (start-server! (config id topic))]
        (try
          (with-open [conn (odradek/connect (:socket server))]
            (binding [*topic* topic
                      *unconfigured* (str "odradek-e2e-clj-unconfigured-" id)
                      *server* server
                      *conn* conn]
              (f)))
          (finally
            (stop-server! server))))
      (finally
        (delete-topic! @kafka-a topic)
        (delete-topic! @kafka-b topic)))))

(use-fixtures :each with-server)

;; ---------------------------------------------------------------------------
;; Helpers

(defn- eventually
  [what f]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (f) (is true what)
        (> (System/nanoTime) deadline) (is false what)
        :else (do (Thread/sleep 20) (recur))))))

(defn- text
  [^bytes bytes]
  (String. bytes StandardCharsets/UTF_8))

(defn- send-all!
  "Sends prefix<from> .. prefix<to> over seven keys and waits for every result."
  [producer prefix from to]
  (let [futures (mapv (fn [i]
                        (odradek/send! producer {:topic *topic*
                                                 :key (str "key-" (mod i 7))
                                                 :value (str prefix i)}))
                      (range from (inc to)))]
    (doseq [^CompletableFuture future futures]
      (is (= ["a" "b"]
             (mapv :cluster (:writes (.get future timeout-ms TimeUnit/MILLISECONDS))))))))

(defn- expected
  [prefix from to]
  (into (sorted-set) (map #(str prefix %)) (range from (inc to))))

(defn- tracking
  "Consumer callbacks that keep `owned` equal to the partitions the consumer has."
  [owned]
  {:on-assigned #(swap! owned into %)
   :on-revoked #(swap! owned (fn [s] (reduce disj s %)))
   :on-lost #(swap! owned (fn [s] (reduce disj s %)))})

;; ---------------------------------------------------------------------------
;; The cases

(deftest produced-records-are-consumed-once-and-in-order-per-key
  (let [seen (atom [])
        by-key (atom {})]
    (with-open [producer (odradek/producer *conn*)
                _consumer (odradek/consumer
                           *conn*
                           {:topics [*topic*]}
                           (fn [record]
                             (let [value (text (:value record))]
                               (swap! seen conj value)
                               (swap! by-key update (text (:key record)) (fnil conj [])
                                      (parse-long (subs value 1))))))]
      (send-all! producer "v" 1 300)
      (eventually "all records handled" #(>= (count @seen) 300)))
    (is (= (expected "v" 1 300) (into (sorted-set) @seen)))
    (is (= 300 (count @seen)) "no duplicates")
    (is (= 7 (count @by-key)))
    (doseq [[key values] @by-key]
      (is (= (sort values) values) (str "in order for " key)))

    ;; Everything was processed, so a new consumer of the group has nothing to read.
    (let [again (atom [])]
      (with-open [_consumer (odradek/consumer *conn*
                                              {:topics [*topic*]}
                                              #(swap! again conj (text (:value %))))]
        (Thread/sleep 3000))
      (is (empty? @again)))))

(deftest unconfigured-topic-fails-the-send-without-breaking-the-producer
  (with-open [producer (odradek/producer *conn*)]
    (let [refused (odradek/send! producer {:topic *unconfigured* :value "v"})
          data (try
                 (.get refused timeout-ms TimeUnit/MILLISECONDS)
                 nil
                 (catch ExecutionException e
                   (ex-data (ex-cause e))))]
      (is (= :produce-failed (:odradek/error data)))
      (is (= :ERROR_CODE_TOPIC_NOT_CONFIGURED (:code data)))
      (is (false? (:outcome-unknown? data)))
      (is (= [] (:writes data))))
    (send-all! producer "v" 1 5)))

(deftest handler-that-throws-stops-the-consumer-and-the-record-is-delivered-again
  (with-open [producer (odradek/producer *conn*)]
    (send-all! producer "v" 1 20))
  (let [first-seen (atom [])
        failure (promise)
        failing (odradek/consumer
                 *conn*
                 {:topics [*topic*]
                  :on-failed #(deliver failure %)}
                 (fn [record]
                   (let [value (text (:value record))]
                     (when (= "v13" value)
                       (throw (IllegalStateException. "cannot process v13")))
                     (swap! first-seen conj value))))]
    (is (= "cannot process v13" (ex-message (deref failure timeout-ms nil))))
    (odradek/close! failing)
    (is (not (some #{"v13"} @first-seen)))

    (let [second-seen (atom [])]
      (with-open [_consumer (odradek/consumer *conn*
                                              {:topics [*topic*]}
                                              #(swap! second-seen conj (text (:value %))))]
        (eventually "v13 is delivered again" #(some #{"v13"} @second-seen))
        (eventually "everything is processed by one consumer or the other"
                    #(= (expected "v" 1 20)
                        (into (sorted-set) (concat @first-seen @second-seen))))))))

(deftest second-consumer-shares-the-group-and-each-record-is-handled-once
  (let [one (atom [])
        two (atom [])
        owned-by-one (atom #{})
        owned-by-two (atom #{})]
    (with-open [producer (odradek/producer *conn*)
                _first (odradek/consumer *conn*
                                         (assoc (tracking owned-by-one) :topics [*topic*])
                                         #(swap! one conj (text (:value %))))]
      (eventually "the first consumer owns every partition"
                  #(= partitions (count @owned-by-one)))
      (with-open [_second (odradek/consumer *conn*
                                            (assoc (tracking owned-by-two) :topics [*topic*])
                                            #(swap! two conj (text (:value %))))]
        (eventually "the partitions are split"
                    #(let [mine @owned-by-one
                           theirs @owned-by-two]
                       (and (seq theirs)
                            (= partitions (+ (count mine) (count theirs)))
                            (empty? (filter mine theirs)))))
        (send-all! producer "v" 1 200)
        (eventually "all records handled" #(>= (+ (count @one) (count @two)) 200))))
    (let [all (concat @one @two)]
      (is (= (expected "v" 1 200) (into (sorted-set) all)))
      (is (= 200 (count all)) "each record was handled by exactly one consumer")
      (is (seq @one))
      (is (seq @two)))))

(deftest producer-and-consumer-survive-a-server-restart
  (let [seen (atom [])
        lost (atom #{})]
    (with-open [producer (odradek/producer *conn*)
                _consumer (odradek/consumer *conn*
                                            {:topics [*topic*]
                                             :on-lost #(swap! lost into %)}
                                            #(swap! seen conj (text (:value %))))]
      (send-all! producer "before" 1 50)
      (eventually "the first records are handled" #(>= (count @seen) 50))

      (restart-server! *server*)

      ;; The same producer and consumer, against a server with a new epoch.
      (send-all! producer "after" 1 50)
      (eventually "the records sent after the restart are handled"
                  #(every? (set @seen) (expected "after" 1 50))))
    (is (= partitions (count @lost)) "the consumer was told its partitions were gone")
    (is (every? (set @seen) (expected "before" 1 50)))
    ;; Records acked just before the restart may not have been committed yet, so
    ;; "before" records can be handled twice. "after" records cannot: nothing
    ;; interrupted them.
    (is (= 50 (count (filter #(str/starts-with? % "after") @seen))))))
