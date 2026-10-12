(ns odradek.client.classpath-test
  "Guards the one risk ADR 2 names: two ways of resolving JVM dependencies on
  one class path.

  The Clojure libraries come from deps.edn through a lockfile; gRPC, Netty,
  protobuf-java and Clojure come from the Bazel module. These tests fail when a
  class file is on the class path twice, when a library is not at the version
  this repository pins, or when deps.edn tells Clojars consumers something
  else than what was tested here."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]])
  (:import [java.io File]
           [java.util.jar JarFile]))

(defn- manifest-class-path
  "The jars a jar names in its manifest. Bazel replaces a long class path with
  one jar that only carries this attribute."
  [^File jar]
  (with-open [jf (JarFile. jar)]
    (when-let [cp (some-> (.getManifest jf)
                          (.getMainAttributes)
                          (.getValue "Class-Path"))]
      (for [entry (str/split (str/trim cp) #"\s+")
            :when (seq entry)]
        (io/file (.resolve (.toURI (.getParentFile (.getAbsoluteFile jar))) ^String entry))))))

(defn- class-path-jars
  []
  (let [entries (->> (str/split (System/getProperty "java.class.path")
                                (re-pattern File/pathSeparator))
                     (map io/file)
                     (filter #(.isFile ^File %)))]
    (->> entries
         (mapcat (fn [jar] (cons jar (manifest-class-path jar))))
         (filter #(and (.isFile ^File %) (str/ends-with? (.getName ^File %) ".jar")))
         (map #(.getCanonicalFile ^File %))
         distinct
         vec)))

(defn- class-entries
  [^File jar]
  (with-open [jf (JarFile. jar)]
    (->> (enumeration-seq (.entries jf))
         (map #(.getName ^java.util.jar.JarEntry %))
         (filter #(str/ends-with? % ".class"))
         (remove #(or (str/starts-with? % "META-INF/")
                      (str/ends-with? % "module-info.class")))
         vec)))

(deftest every-class-is-on-the-class-path-once
  (let [jars (class-path-jars)
        owners (reduce (fn [acc ^File jar]
                         (reduce (fn [acc entry] (update acc entry (fnil conj []) (.getName jar)))
                                 acc
                                 (class-entries jar)))
                       {}
                       jars)
        twice (->> owners
                   (filter (fn [[_ jars]] (> (count jars) 1)))
                   (map (fn [[entry jars]] [(vec (sort jars)) entry]))
                   (group-by first))]
    (testing "the test saw a real class path"
      (is (< 20 (count jars)))
      (is (contains? owners "io/grpc/ManagedChannel.class"))
      (is (contains? owners "io/grpc/netty/NettyChannelBuilder.class"))
      (is (contains? owners "io/netty/channel/epoll/Epoll.class"))
      (is (contains? owners "com/google/protobuf/Message.class"))
      (is (contains? owners "clojure/lang/RT.class"))
      (is (contains? owners "io/github/bpalermo/odradek/client/core/ProduceCore.class")))
    (is (empty? twice)
        (str "classes in more than one jar: "
             (pr-str (into {} (map (fn [[jars entries]] [jars (count entries)])) twice))))))

(deftest clojure-libraries-ship-as-source-once
  (doseq [resource ["clj_grpc/client.clj" "clj_protobuf/core.clj" "clojure/core.clj"]]
    (is (= 1 (count (enumeration-seq
                     (.getResources (.getContextClassLoader (Thread/currentThread)) resource))))
        resource)))

(defn- netty-versions
  []
  (->> (io.netty.util.Version/identify)
       vals
       (map #(.artifactVersion ^io.netty.util.Version %))
       set))

(defn- protobuf-version
  []
  (str com.google.protobuf.RuntimeVersion/MAJOR "."
       com.google.protobuf.RuntimeVersion/MINOR "."
       com.google.protobuf.RuntimeVersion/PATCH))

(deftest libraries-are-at-the-versions-the-module-pins
  (is (= "1.84.0" io.grpc.internal.GrpcUtil/IMPLEMENTATION_VERSION))
  (is (= #{"4.2.16.Final"} (netty-versions)))
  (is (= "4.36.2" (protobuf-version))))

(deftest deps-edn-says-what-was-tested
  (let [deps-edn (edn/read-string (slurp "client/clojure/deps.edn"))
        top (:deps deps-edn)
        bazel (get-in deps-edn [:aliases :bazel :replace-deps])
        version (fn [lib] (get-in top [lib :mvn/version]))]
    (testing "the pom's pins are the versions on this class path"
      (doseq [[lib _] top
              :when (= "io.grpc" (namespace lib))]
        (is (= io.grpc.internal.GrpcUtil/IMPLEMENTATION_VERSION (version lib)) lib))
      (doseq [[lib _] top
              :when (= "com.google.protobuf" (namespace lib))]
        (is (= (protobuf-version) (version lib)) lib))
      (is (= (clojure-version) (version 'org.clojure/clojure))))
    (testing "the build takes the same Clojure libraries the pom names"
      (is (= #{'com.github.bpalermo/clj-grpc 'com.github.bpalermo/clj-protobuf}
             (set (keys bazel))))
      (doseq [[lib coord] bazel]
        (is (= (version lib) (:mvn/version coord)) lib)))
    (testing "the build takes no JVM library from deps.edn"
      (let [lock (slurp "client/clojure/deps.lock.json")
            coordinates (map second (re-seq #"\"coordinates\": \"([^\"]+)\"" lock))]
        (is (seq coordinates))
        (doseq [c coordinates]
          (is (or (str/starts-with? c "com.github.bpalermo/")
                  (str/starts-with? c "org.clojure/"))
              c))
        (doseq [[lib coord] bazel]
          (is (some #{(str lib ":" (:mvn/version coord))} coordinates) lib))))))
