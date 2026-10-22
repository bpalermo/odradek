(ns odradek.client-test
  "The public API against a stand-in server: clj-grpc's server side on a Unix
  socket, speaking just enough of the API for each case. The streams, the
  reconnects and the handler thread are the real ones; there is no Kafka and
  no Odradek server. The end-to-end suite (//e2e) covers those."
  (:require [clj-grpc.server :as server]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [odradek.client :as odradek]
            [odradek.client.stream :as stream]
            [odradek.kafka.v1.odradek :as api])
  (:import [io.grpc Server Status]
           [java.nio.charset StandardCharsets]
           [java.nio.file Files Path]
           [java.nio.file.attribute FileAttribute]
           [java.util.concurrent CompletableFuture ExecutionException TimeUnit]
           [java.util.function BiConsumer]))

(def ^:private timeout-ms 30000)

(def ^:private ^:dynamic *socket* nil)

(defn- with-socket
  [f]
  ;; Under /tmp: the path of a Unix socket is limited to about a hundred bytes.
  (let [dir (Files/createTempDirectory (Path/of "/tmp" (make-array String 0))
                                       "odradek-clj"
                                       (make-array FileAttribute 0))]
    (try
      (binding [*socket* (str dir "/kafka.sock")]
        (f))
      (finally
        (doseq [file (reverse (file-seq (io/file (str dir))))]
          (io/delete-file file true))))))

(use-fixtures :each with-socket)

(defn- text
  [^bytes bytes]
  (when bytes
    (String. bytes StandardCharsets/UTF_8)))

(defn- eventually
  [what f]
  (let [deadline (+ (System/nanoTime) (* 1000000 timeout-ms))]
    (loop []
      (cond
        (f) (is true what)
        (> (System/nanoTime) deadline) (is false what)
        :else (do (Thread/sleep 10) (recur))))))

(defn- ex-data-of
  "The data a failed send carries."
  [^CompletableFuture future]
  (try
    (.get future timeout-ms TimeUnit/MILLISECONDS)
    nil
    (catch ExecutionException e
      (ex-data (ex-cause e)))))

(defn- thrown-data
  [f]
  (try
    (f)
    nil
    (catch clojure.lang.ExceptionInfo e
      (ex-data e))))

;; ---------------------------------------------------------------------------
;; The stand-in server

(defn- start-server
  [handlers]
  (io/delete-file *socket* true)
  (server/start
   (server/server
    {:services [{:service api/OdradekService
                 :handlers (merge {:produce (fn [_ _] {})
                                   :consume (fn [_ _] {})}
                                  handlers)}]
     :address {:unix *socket*}
     :health false})))

(defn- kill-server!
  "Stops a server the way a crash does: streams break, nothing is completed."
  [{:keys [^Server server]}]
  (.shutdownNow server)
  (.awaitTermination server 10 TimeUnit/SECONDS))

(defn- produce-handler
  "A Produce stream that announces `window` and answers each record with what
  `answer` returns for it, or not at all when that is nil. Everything received
  goes to `log`."
  [{:keys [epoch window log answer]}]
  (fn [send! close!]
    {:on-next (fn [request]
                (let [{:keys [open record]} (api/proto->ProduceRequest request)]
                  (when open
                    (swap! log conj [:open open])
                    (send! (api/ProduceResponse->proto
                            {:ready {:server-epoch epoch
                                     :max-in-flight-records window
                                     :max-in-flight-bytes 1000000}})))
                  (when record
                    (swap! log conj [:record record])
                    (when-let [result (answer record)]
                      (send! (api/ProduceResponse->proto {:result result}))))))
     :on-complete (fn [] (close!))}))

(defn- written
  [record]
  {:seq (:seq record)
   :writes [{:cluster "a"
             :topic (:topic record)
             :partition 0
             :offset (:seq record)
             :timestamp-ms 1}]})

(defn- records-in
  [log]
  (keep (fn [[kind message]] (when (= :record kind) message)) @log))

(defn- consume-handler
  "A Consume stream that calls `on-subscribe` with its response side when the
  handshake arrives. Everything received goes to `log`, and :completed when
  the client completes the stream."
  [{:keys [log on-subscribe]}]
  (fn [send! close!]
    {:on-next (fn [request]
                (let [message (api/proto->ConsumeRequest request)]
                  (swap! log conj message)
                  (when (:subscribe message)
                    (on-subscribe send! close!))))
     :on-complete (fn []
                    (swap! log conj :completed)
                    (close!))}))

(defn- subscribed!
  [send!]
  (send! (api/ConsumeResponse->proto
          {:subscribed {:server-epoch "e1" :max-processing-ms 1000 :cluster "a" :group-id "g"}}))
  (send! (api/ConsumeResponse->proto
          {:assigned {:partitions [{:topic "t" :partition 0 :assignment-epoch 1}]}})))

(defn- deliver!
  [send! offsets]
  (send! (api/ConsumeResponse->proto
          {:records {:topic "t"
                     :partition 0
                     :assignment-epoch 1
                     :records (mapv (fn [offset]
                                      {:offset offset
                                       :timestamp-ms 1
                                       :key (.getBytes "k" StandardCharsets/UTF_8)
                                       :value (.getBytes (str "v" offset) StandardCharsets/UTF_8)})
                                    offsets)}})))

(defn- acked-offsets
  [log]
  (->> @log
       (keep :ack)
       (mapv (juxt :first-offset :last-offset :assignment-epoch))))

;; ---------------------------------------------------------------------------
;; Produce

(deftest send-resolves-when-the-server-answers
  (let [log (atom [])
        server (start-server {:produce (produce-handler {:epoch "e1" :window 10 :log log
                                                         :answer written})})]
    (try
      (with-open [conn (odradek/connect *socket*)
                  producer (odradek/producer conn)]
        (let [futures (mapv #(odradek/send! producer {:topic "t" :key (str "k" %) :value (str "v" %)})
                            (range 3))]
          (is (= [{:writes [{:cluster "a" :topic "t" :partition 0 :offset 1 :timestamp-ms 1}]}
                  {:writes [{:cluster "a" :topic "t" :partition 0 :offset 2 :timestamp-ms 1}]}
                  {:writes [{:cluster "a" :topic "t" :partition 0 :offset 3 :timestamp-ms 1}]}]
                 (mapv #(deref % timeout-ms :timeout) futures)))))
      (let [[kind open] (first @log)]
        (is (= :open kind))
        (is (seq (:session-id open)))
        (is (= {:language "clojure" :version stream/version}
               (select-keys (:client open) [:language :version]))))
      (is (= [[1 "k0" "v0"] [2 "k1" "v1"] [3 "k2" "v2"]]
             (mapv (juxt :seq (comp text :key) (comp text :value)) (records-in log))))
      (finally
        (kill-server! server)))))

(deftest failed-send-carries-the-error-and-leaves-the-producer-usable
  (let [log (atom [])
        answer (fn [record]
                 (if (= "unconfigured" (:topic record))
                   {:seq (:seq record)
                    :error {:code :ERROR_CODE_TOPIC_NOT_CONFIGURED :message "no such producer"}}
                   (written record)))
        server (start-server {:produce (produce-handler {:epoch "e1" :window 10 :log log
                                                         :answer answer})})]
    (try
      (with-open [conn (odradek/connect *socket*)
                  producer (odradek/producer conn)]
        (is (= {:odradek/error :produce-failed
                :code :ERROR_CODE_TOPIC_NOT_CONFIGURED
                :message "no such producer"
                :kafka-exception ""
                :outcome-unknown? false
                :writes []}
               (ex-data-of (odradek/send! producer {:topic "unconfigured" :value "v"}))))
        (is (= 1 (count (:writes (deref (odradek/send! producer {:topic "t" :value "v"})
                                        timeout-ms :timeout))))))
      (finally
        (kill-server! server)))))

(deftest send-blocks-up-to-max-block-when-the-window-is-full
  (let [log (atom [])
        server (start-server {:produce (produce-handler {:epoch "e1" :window 1 :log log
                                                         :answer (constantly nil)})})]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [producer (odradek/producer conn {:max-block-ms 300})
              first-send (odradek/send! producer {:topic "t" :value "v1"})]
          (eventually "the server announced its window of one record and holds it"
                      #(= 1 (count (records-in log))))
          (let [started (System/nanoTime)
                data (thrown-data #(odradek/send! producer {:topic "t" :value "v2"}))
                waited-ms (quot (- (System/nanoTime) started) 1000000)]
            (is (= :buffer-full (:odradek/error data)))
            (is (<= 300 waited-ms)))
          (is (not (.isDone first-send)))
          (is (= 1 (count (records-in log))))))
      (finally
        (kill-server! server)))))

(deftest closed-producer-refuses-records
  (let [server (start-server {:produce (produce-handler {:epoch "e1" :window 10 :log (atom [])
                                                         :answer written})})]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [producer (odradek/producer conn)]
          (is (map? (deref (odradek/send! producer {:topic "t" :value "v"}) timeout-ms :timeout)))
          (odradek/close! producer)
          (is (= :closed
                 (:odradek/error (thrown-data #(odradek/send! producer {:topic "t" :value "v"})))))))
      (finally
        (kill-server! server)))))

(deftest close-returns-once-the-last-result-has-reached-what-was-chained-on-it
  (let [answer (promise)
        server (start-server {:produce (produce-handler
                                        {:epoch "e1" :window 10 :log (atom [])
                                         :answer (fn [record] @answer (written record))})})]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [producer (odradek/producer conn)
              answered (atom false)]
          (.whenComplete ^CompletableFuture (odradek/send! producer {:topic "t" :value "v"})
                         (reify BiConsumer
                           (accept [_ result _]
                             ;; Slow bookkeeping on the result: close! must wait for it.
                             (Thread/sleep 300)
                             (reset! answered (map? result)))))
          (deliver answer true)
          (odradek/close! producer)
          (is @answered)))
      (finally
        (kill-server! server)))))

(deftest close-called-from-what-was-chained-on-the-last-result-returns-at-once
  (let [answer (promise)
        server (start-server {:produce (produce-handler
                                        {:epoch "e1" :window 10 :log (atom [])
                                         :answer (fn [record] @answer (written record))})})]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [producer (odradek/producer conn)
              took-ms (promise)]
          (.whenComplete ^CompletableFuture (odradek/send! producer {:topic "t" :value "v"})
                         (reify BiConsumer
                           (accept [_ _ _]
                             ;; Close after the last ack: this completion is on the
                             ;; stack, and close! must not wait for it.
                             (let [begun (System/nanoTime)]
                               (odradek/close! producer)
                               (deliver took-ms (quot (- (System/nanoTime) begun) 1000000))))))
          (deliver answer true)
          (is (< (long (deref took-ms timeout-ms Long/MAX_VALUE)) 2000))))
      (finally
        (kill-server! server)))))

(deftest unanswered-record-is-resent-to-the-same-server-epoch
  (let [first-log (atom [])
        second-log (atom [])
        first-server (start-server {:produce (produce-handler {:epoch "e1" :window 10
                                                               :log first-log
                                                               :answer (constantly nil)})})]
    (with-open [conn (odradek/connect *socket*)
                producer (odradek/producer conn)]
      (let [future (odradek/send! producer {:topic "t" :value "v"})]
        (eventually "the record reached the server" #(= 1 (count (records-in first-log))))
        ;; The stream breaks and a server that still knows the session comes back.
        (kill-server! first-server)
        (let [second-server (start-server {:produce (produce-handler {:epoch "e1" :window 10
                                                                      :log second-log
                                                                      :answer written})})]
          (try
            (is (= {:writes [{:cluster "a" :topic "t" :partition 0 :offset 1 :timestamp-ms 1}]}
                   (deref future timeout-ms :timeout)))
            (testing "under the seq it was first sent with, which is what lets the server dedupe"
              (is (= [1] (mapv :seq (records-in second-log)))))
            (testing "by the same session"
              (is (= (:session-id (second (first @first-log)))
                     (:session-id (second (first @second-log))))))
            (finally
              (kill-server! second-server))))))))

(deftest unanswered-record-is-outcome-unknown-after-a-server-restart
  (let [first-log (atom [])
        second-log (atom [])
        first-server (start-server {:produce (produce-handler {:epoch "e1" :window 10
                                                               :log first-log
                                                               :answer (constantly nil)})})]
    (with-open [conn (odradek/connect *socket*)
                producer (odradek/producer conn)]
      (let [future (odradek/send! producer {:topic "t" :value "before"})]
        (eventually "the record reached the server" #(= 1 (count (records-in first-log))))
        (kill-server! first-server)
        ;; A new server epoch: the session and its deduplication state are gone.
        (let [second-server (start-server {:produce (produce-handler {:epoch "e2" :window 10
                                                                      :log second-log
                                                                      :answer written})})]
          (try
            (let [data (ex-data-of future)]
              (is (= :ERROR_CODE_OUTCOME_UNKNOWN (:code data)))
              (is (true? (:outcome-unknown? data))))
            (testing "the same producer keeps working against the new server"
              (is (map? (deref (odradek/send! producer {:topic "t" :value "after"})
                               timeout-ms :timeout)))
              (is (= ["after"] (mapv (comp text :value) (records-in second-log)))))
            (finally
              (kill-server! second-server))))))))

;; ---------------------------------------------------------------------------
;; Consume

(deftest handler-gets-each-record-and-its-return-acks-it
  (let [log (atom [])
        response (promise)
        server (start-server {:consume (consume-handler
                                        {:log log
                                         :on-subscribe (fn [send! _]
                                                         (subscribed! send!)
                                                         (deliver! send! [0 1 2])
                                                         (deliver response send!))})})
        seen (atom [])
        events (atom [])]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [consumer (odradek/consumer
                        conn
                        {:topics ["t"]
                         :max-processing-ms 1000
                         :on-assigned #(swap! events conj [:assigned %])
                         :on-revoked #(swap! events conj [:revoked %])
                         :on-lost #(swap! events conj [:lost %])}
                        (fn [record]
                          (swap! seen conj record)))]
          (eventually "the three records are acked one by one"
                      #(= [[0 0 1] [1 1 1] [2 2 1]] (acked-offsets log)))
          (is (= [{:topic "t" :partition 0 :offset 0 :timestamp-ms 1 :key "k" :value "v0"}
                  {:topic "t" :partition 0 :offset 1 :timestamp-ms 1 :key "k" :value "v1"}
                  {:topic "t" :partition 0 :offset 2 :timestamp-ms 1 :key "k" :value "v2"}]
                 (mapv #(-> (select-keys % [:topic :partition :offset :timestamp-ms :key :value])
                            (update :key text)
                            (update :value text))
                       @seen)))
          (is (= [] (:headers (first @seen))))
          (testing "the handshake names the topics and this library"
            (let [subscribe (:subscribe (first @log))]
              (is (= ["t"] (:topics subscribe)))
              (is (= 1000 (:max-processing-ms subscribe)))
              (is (= "clojure" (:language (:client subscribe))))))
          (testing "credits were granted before anything was delivered"
            (is (pos? (reduce + (keep #(get-in % [:credits :records]) @log)))))

          (testing "a revocation is reported and then answered"
            (@response (api/ConsumeResponse->proto
                        {:revoked {:revoke-id 5
                                   :partitions [{:topic "t" :partition 0}]
                                   :deadline-ms 5000}}))
            (eventually "the server got RevokeDone"
                        #(some (fn [m] (= 5 (get-in m [:revoke-done :revoke-id]))) @log))
            (is (= [[:assigned [{:topic "t" :partition 0}]]
                    [:revoked [{:topic "t" :partition 0}]]]
                   @events)))

          (testing "closing completes the stream"
            (odradek/close! consumer)
            (is (= :completed (last @log))))))
      (finally
        (kill-server! server)))))

(deftest handler-that-throws-stops-the-consumer-before-the-next-record
  (let [log (atom [])
        server (start-server {:consume (consume-handler
                                        {:log log
                                         :on-subscribe (fn [send! _]
                                                         (subscribed! send!)
                                                         (deliver! send! [0 1 2]))})})
        seen (atom [])
        failure (promise)]
    (try
      (with-open [conn (odradek/connect *socket*)]
        (let [consumer (odradek/consumer
                        conn
                        {:topics ["t"]
                         :on-failed #(deliver failure %)}
                        (fn [record]
                          (swap! seen conj (:offset record))
                          (when (= 1 (:offset record))
                            (throw (IllegalStateException. "cannot process 1")))))]
          (is (= "cannot process 1" (ex-message (deref failure timeout-ms nil))))
          (eventually "the consumer left the stream on its own" #(= :completed (last @log)))
          (is (= [0 1] @seen))
          (testing "only what the handler returned for is acked"
            (is (= [[0 0 1]] (acked-offsets log))))
          (odradek/close! consumer)))
      (finally
        (kill-server! server)))))

(deftest partitions-are-lost-when-the-stream-breaks-and-the-consumer-comes-back
  (let [first-log (atom [])
        second-log (atom [])
        subscribing (fn [log offsets]
                      (consume-handler {:log log
                                        :on-subscribe (fn [send! _]
                                                        (subscribed! send!)
                                                        (deliver! send! offsets))}))
        first-server (start-server {:consume (subscribing first-log [0])})
        seen (atom [])
        events (atom [])]
    (with-open [conn (odradek/connect *socket*)
                _consumer (odradek/consumer
                           conn
                           {:topics ["t"]
                            :on-assigned #(swap! events conj [:assigned %])
                            :on-lost #(swap! events conj [:lost %])}
                           (fn [record]
                             (swap! seen conj (:offset record))))]
      (eventually "the first record is acked" #(= [[0 0 1]] (acked-offsets first-log)))
      (kill-server! first-server)
      (eventually "the partition is reported lost"
                  #(= [[:assigned [{:topic "t" :partition 0}]]
                       [:lost [{:topic "t" :partition 0}]]]
                      @events))
      (let [second-server (start-server {:consume (subscribing second-log [1])})]
        (try
          (eventually "the same consumer subscribed again and went on"
                      #(= [[1 1 1]] (acked-offsets second-log)))
          (is (= [0 1] @seen))
          (is (= [:assigned :lost :assigned] (mapv first @events)))
          (finally
            (kill-server! second-server)))))))

(deftest refused-subscription-fails-the-consumer
  (let [server (start-server
                {:consume (consume-handler
                           {:log (atom [])
                            :on-subscribe (fn [_ _]
                                            (throw (.asRuntimeException
                                                    (.withDescription Status/NOT_FOUND
                                                                      "topic t is not declared"))))})})
        failure (promise)]
    (try
      (with-open [conn (odradek/connect *socket*)
                  _consumer (odradek/consumer conn
                                              {:topics ["t"] :on-failed #(deliver failure %)}
                                              (fn [_] nil))]
        (let [data (ex-data (deref failure timeout-ms nil))]
          (is (= :consumer-failed (:odradek/error data)))
          (is (= "NOT_FOUND" (:status-code data)))))
      (finally
        (kill-server! server)))))

(deftest consumer-options-are-checked
  (with-open [conn (odradek/connect *socket*)]
    (is (= :invalid-options
           (:odradek/error (thrown-data #(odradek/consumer conn {:topics []} (fn [_] nil))))))
    (is (= :invalid-options
           (:odradek/error (thrown-data #(odradek/consumer conn {:topics ["t"]} nil)))))))
