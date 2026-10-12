(ns odradek.client.records-test
  (:require [clojure.test :refer [deftest is testing]]
            [odradek.client.records :as records]
            [odradek.client.stream :as stream]
            [odradek.kafka.v1.odradek :as api])
  (:import [com.google.protobuf ByteString]
           [io.github.bpalermo.odradek.kafka.v1 ClientInfo ConsumedRecord ErrorCode Header
            ProduceRecord ProduceRequest ProduceResult RecordBatch RecordMetadata]
           [java.nio.charset StandardCharsets]))

(defn- text
  [^bytes bytes]
  (String. bytes StandardCharsets/UTF_8))

;; A contract type for the tests: ClientInfo is a message with two strings.
(def ^:private client-info-codec
  (records/proto-codec api/ClientInfo-prototype api/ClientInfo->proto api/proto->ClientInfo))

(deftest generated-namespace-uses-the-java-classes-of-the-core
  (testing "so a message goes between clj-grpc and the Java core as it is"
    (is (instance? ProduceRequest api/ProduceRequest-prototype))
    (is (instance? ProduceRecord (api/ProduceRecord->proto {:topic "t"})))
    (is (instance? ProduceRequest
                   (.getDefaultInstanceForType
                    ^com.google.protobuf.Message
                    (:input-prototype (:produce api/odradekservice-methods)))))))

(deftest client-info-names-the-language
  (is (= "clojure" (.getLanguage stream/client-info)))
  (is (= stream/version (.getVersion stream/client-info)))
  (is (seq stream/version)))

(deftest produce-record-from-a-map
  (testing "bytes, strings and absence"
    (let [record (records/produce-record
                  nil
                  {:topic "t"
                   :key "k"
                   :value (.getBytes "v" StandardCharsets/UTF_8)
                   :headers [{:key "a" :value "1"} {:key "b"} {:key "a" :value (byte-array [2])}]
                   :partition 3
                   :timestamp-ms 99})]
      (is (= "t" (.getTopic record)))
      (is (= "k" (.toStringUtf8 (.getKey record))))
      (is (= "v" (.toStringUtf8 (.getValue record))))
      (is (= 3 (.getPartition record)))
      (is (= 99 (.getTimestampMs record)))
      (is (= ["a" "b" "a"] (mapv #(.getKey ^Header %) (.getHeadersList record))))
      (is (= [true false true] (mapv #(.hasValue ^Header %) (.getHeadersList record))))
      (is (= "1" (.toStringUtf8 (.getValue ^Header (first (.getHeadersList record))))))))
  (testing "nothing but a topic is a null-keyed tombstone for the partitioner to place"
    (let [record (records/produce-record nil {:topic "t"})]
      (is (not (.hasKey record)))
      (is (not (.hasValue record)))
      (is (not (.hasPartition record)))
      (is (not (.hasTimestampMs record)))
      (is (zero? (.getHeadersCount record)))))
  (testing "an empty value is not a tombstone"
    (is (.hasValue (records/produce-record nil {:topic "t" :value ""}))))
  (testing "a protobuf message is sent in protobuf encoding"
    (let [message (-> (ClientInfo/newBuilder) (.setLanguage "x") (.build))
          record (records/produce-record nil {:topic "t" :value message})]
      (is (= message (ClientInfo/parseFrom (.getValue record))))))
  (testing "a map or a record of the topic's contract type is encoded with clj-protobuf"
    (let [contracts {"t" {:key client-info-codec :value client-info-codec}}
          record (records/produce-record contracts
                                         {:topic "t"
                                          :key {:language "k"}
                                          :value (api/map->ClientInfo {:language "clojure"
                                                                       :version "1"})})]
      (is (= "k" (.getLanguage (ClientInfo/parseFrom (.getKey record)))))
      (is (= "1" (.getVersion (ClientInfo/parseFrom (.getValue record)))))))
  (testing "what cannot be encoded is refused before anything is sent"
    (is (= :cannot-encode
           (:odradek/error
            (try (records/produce-record nil {:topic "t" :value {:language "x"}})
                 (catch clojure.lang.ExceptionInfo e (ex-data e))))))
    (is (= :invalid-record
           (:odradek/error
            (try (records/produce-record nil {:value "v"})
                 (catch clojure.lang.ExceptionInfo e (ex-data e))))))))

(defn- batch
  ^RecordBatch [^ConsumedRecord record]
  (-> (RecordBatch/newBuilder)
      (.setTopic "t")
      (.setPartition 2)
      (.setAssignmentEpoch 7)
      (.addRecords record)
      (.build)))

(deftest received-record-as-a-map
  (testing "bytes without a contract; offset zero and null values are kept apart"
    (let [record (-> (ConsumedRecord/newBuilder)
                     (.setOffset 0)
                     (.setTimestampMs 5)
                     (.setKey (ByteString/copyFromUtf8 "k"))
                     (.addHeaders (-> (Header/newBuilder) (.setKey "h")))
                     (.addHeaders (-> (Header/newBuilder)
                                      (.setKey "h")
                                      (.setValue (ByteString/copyFromUtf8 "x"))))
                     (.build))
          received (records/received nil (batch record) record)]
      (is (= {:topic "t" :partition 2 :offset 0 :timestamp-ms 5}
             (select-keys received [:topic :partition :offset :timestamp-ms])))
      (is (= "k" (text (:key received))))
      (is (nil? (:value received)))
      (is (contains? received :value))
      (is (= ["h" "h"] (mapv :key (:headers received))))
      (is (nil? (:value (first (:headers received)))))
      (is (= "x" (text (:value (second (:headers received))))))
      (is (not (contains? received :decode-error)))))
  (testing "a contract decodes the value into the generated record"
    (let [value (api/ClientInfo->proto {:language "clojure" :version "1"})
          record (-> (ConsumedRecord/newBuilder)
                     (.setOffset 4)
                     (.setValue (.toByteString ^ClientInfo value))
                     (.build))
          received (records/received {"t" {:value client-info-codec}} (batch record) record)]
      (is (= (api/map->ClientInfo {:language "clojure" :version "1"}) (:value received)))
      (is (nil? (:key received)))))
  (testing "a record the server could not decode is handed over as stored"
    (let [record (-> (ConsumedRecord/newBuilder)
                     (.setOffset 4)
                     (.setValue (ByteString/copyFromUtf8 "not protobuf"))
                     (.setDecodeError "bad")
                     (.build))
          received (records/received {"t" {:value client-info-codec}} (batch record) record)]
      (is (= "bad" (:decode-error received)))
      (is (= "not protobuf" (text (:value received)))))))

(defn- metadata
  [cluster partition offset]
  (-> (RecordMetadata/newBuilder)
      (.setCluster cluster)
      (.setTopic "t")
      (.setPartition (int partition))
      (.setOffset (long offset))
      (.setTimestampMs 9)))

(deftest send-outcome-is-data
  (testing "success lists every cluster that wrote the record"
    (is (= {:writes [{:cluster "a" :topic "t" :partition 0 :offset 0 :timestamp-ms 9}
                     {:cluster "b" :topic "t" :partition 1 :offset 12 :timestamp-ms 9}]}
           (records/send-outcome (-> (ProduceResult/newBuilder)
                                     (.setSeq 1)
                                     (.addWrites (metadata "a" 0 0))
                                     (.addWrites (metadata "b" 1 12))
                                     (.build))))))
  (testing "failure carries the code and the clusters that wrote it anyway"
    (let [outcome (records/send-outcome
                   (-> (ProduceResult/newBuilder)
                       (.setSeq 1)
                       (.setError (-> (io.github.bpalermo.odradek.kafka.v1.Error/newBuilder)
                                      (.setCode ErrorCode/ERROR_CODE_OUTCOME_UNKNOWN)
                                      (.setMessage "b did not answer")
                                      (.setKafkaException "TimeoutException")))
                       (.addWrites (metadata "a" 0 3))
                       (.build)))]
      (is (instance? clojure.lang.ExceptionInfo outcome))
      (is (= {:odradek/error :produce-failed
              :code :ERROR_CODE_OUTCOME_UNKNOWN
              :message "b did not answer"
              :kafka-exception "TimeoutException"
              :outcome-unknown? true
              :writes [{:cluster "a" :topic "t" :partition 0 :offset 3 :timestamp-ms 9}]}
             (ex-data outcome)))))
  (testing "a definite failure is not outcome unknown"
    (let [outcome (records/send-outcome
                   (-> (ProduceResult/newBuilder)
                       (.setError (-> (io.github.bpalermo.odradek.kafka.v1.Error/newBuilder)
                                      (.setCode ErrorCode/ERROR_CODE_TOPIC_NOT_CONFIGURED)))
                       (.build)))]
      (is (= :ERROR_CODE_TOPIC_NOT_CONFIGURED (:code (ex-data outcome))))
      (is (false? (:outcome-unknown? (ex-data outcome))))
      (is (= [] (:writes (ex-data outcome)))))))

(deftest backoff-doubles-with-jitter-up-to-five-seconds
  (let [backoff (stream/backoff)
        delays (vec (repeatedly 9 #(stream/next-delay-ms! backoff)))
        ceilings [100 200 400 800 1600 3200 5000 5000 5000]]
    (doseq [[delay ceiling] (map vector delays ceilings)]
      (is (<= (quot ceiling 2) delay ceiling)))
    (stream/reset-backoff! backoff)
    (is (<= 50 (stream/next-delay-ms! backoff) 100))))
