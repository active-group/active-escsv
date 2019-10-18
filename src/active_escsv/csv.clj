(ns active-escsv.csv
  (:require [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [active-escsv.log :as log])
  (:import [java.io File OutputStreamWriter BufferedWriter BufferedReader]
           [java.util.zip GZIPOutputStream GZIPInputStream]))

(defn ends-with-gz?
  [s]
  (re-find #"\.gz$" s))

(def parse-csv csv/parse-csv)

(defn read-csv
  [csv-path]
  (let [csv (vec (parse-csv (slurp csv-path)))
        headers (map clojure.string/lower-case (first csv))]
    (mapv (fn [x]
            (into {} (map (fn [a b] [a b])
                          headers
                          x)))
          (rest csv))))

(defn write-csv [filename row-data & [columns]]
  (let [columns (or columns (apply clojure.set/union (map (comp set keys) row-data)))
        headers (map name columns)
        rows (sort-by first (mapv (fn [r] (mapv #(str (r %)) columns)) row-data))]
    (with-open [wrt (java.io.OutputStreamWriter. (io/output-stream filename) "UTF-8")]
      (.write wrt (csv/write-csv (cons headers rows) :force-quote true)))))

(defn write-csv-loop
  [filename columns es-count from-elasticsearch-chan]
  (log/info "Starting writing CSV loop.")
  (async/go
    (let [headers (map name columns)
          gz? (ends-with-gz? filename)]
      (println "FICKEN" (str "|" filename "|") (ends-with-gz? filename))
      (when gz? (log/info "GZipping" filename))
      (with-open [out (io/output-stream filename)
                  zip (if gz?
                        (java.util.zip.GZIPOutputStream. out)
                        out)
                  wrt (java.io.BufferedWriter.
                       (java.io.OutputStreamWriter.
                        zip "UTF-8"))]
        (.write wrt (csv/write-csv [headers] :force-quote true))
        (loop [total-count 0]
          (log/debug "Receiving hits (" total-count "/" es-count ")")
          (if-let [row-data (async/<! from-elasticsearch-chan)]
            (let [rows (mapv (fn [row] (mapv #(str (get row %)) columns)) row-data)
                  cnt (count rows)
                  new-total-count (+ total-count cnt)]
              (log/debug "Writing" cnt "hits (" new-total-count "/" es-count ")")
              (.write wrt (csv/write-csv rows :force-quote true))
              (log/debug "Wrote" (count rows) "hits (" new-total-count "/" es-count ")")
              (recur new-total-count))))))
    (log/info "Done writing CSV loop.")))

(defn read-csv-loop
  [filename channel]
  (log/info "Starting reading CSV loop.")
  (let [gz? (ends-with-gz? filename)
        _ (when gz? (log/info "GUnZipping" filename))
        in (clojure.java.io/input-stream filename)
        maybe-zip  (if gz?
                     (java.util.zip.GZIPInputStream. in)
                     in)
        reader (java.io.BufferedReader. (java.io.InputStreamReader. maybe-zip))
        ch (async/chan)
        raw-data (csv/parse-csv reader)
        headers (first raw-data)]
     [headers
      (async/go
        (doseq [data (partition-all 1000 (map (fn [row]
                                                ;; export saves all fields --
                                                ;; remove empty fields here to
                                                ;; avoid a bunch of unneeded
                                                ;; data in Elasticsearch
                                                (reduce-kv (fn [m k v]
                                                             (if (empty? v)
                                                               m
                                                               (assoc m k v)))
                                                           {}
                                                           (zipmap headers row)))
                                               (rest raw-data)))]
          (async/>! channel data))
        (.close reader)
        (when gz? (.close maybe-zip))
        (.close in)
        (log/info "Done reading CSV loop."))]))
