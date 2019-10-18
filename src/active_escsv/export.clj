(ns active-escsv.export
  (:require [clojure.core.async :as async]
            [clojure.tools.cli :as cli]
            [clojure.set :as set]
            [qbits.spandex :as s]
            [clj-time.core :as time-core]
            [clj-time.format :as time-format]
            [active-escsv.csv :as csv]
            [active-escsv.elasticsearch :as es]
            [active-escsv.log :as log])
  (:gen-class))

(def opts
  [["-?" "--help" "Show this help about the command line arguments."]
   ["-i" "--index-pattern INDEX" "Specify Elasticsearch index pattern (default: *)."
    :default "*"]
   ["-t" "--type TYPE" "Specify Elasticsearch type (default: event)."
    :default "event"]
   ["-h" "--host HOST" "Specify Elasticsearch host (default: localhost)."
    :default "localhost"]
   ["-p" "--port PORT" "Specify Elasticsearch port (default: localhost)."
    :default "9200"]
   ["-q" "--query QUERY" "Elasticsearch query, default is everything."
    :default "*"]
   ["-f" "--from TIMERANGE" "Elasticsearch time range (default is one minute: now-1m)."
    :default "now-1m"]
   ["-l" "--to TIMERANGE" "Elasticsearch time range (default: now)."
    :default "now"]
   ["-o" "--output-file FILENAME" "Filename to write exported data to.  If file ends with `.csv.gz` it is gzipped on the fly, if it ends with `.csv` it is not zipped."
    :default "out.csv.gz"]])

(defn cleanup
  [client index]
  (s/request client {:method :delete :url [index]}))

(defn main
  [{:keys [options arguments summary errors]}]
  (log/init!)
  (cond
    errors
    (do
      (println errors)
      (System/exit 1))

    (:help options)
    (do
      (println summary)
      (System/exit 0))

    :else
    (let [host-port (str (:host options) ":" (:port options))
          client (es/client {:hosts [host-port]
                             :max-retry-timeout 60000})
          index (:index-pattern options)
          type (:type options)
          query-string (:query options)
          output-file (:output-file options)
          query
          {:bool
           {:must [{:range {"@timestamp" {:gte (:from options) :lte (:to options)}}}
                   {:query_string
                    {:query query-string}}]}}]

      (log/info "Exporting" query "from" host-port "to" output-file)

      (let [headers (es/get-headers client index)
            es-count (es/get-count client index query)
            channel (async/chan)
            csv-writer (csv/write-csv-loop output-file headers es-count channel)
            es-reader (es/get-data-loop client index query es-count channel)]
        (log/debug "Waiting for Elasticsearch reader to be done")
        (async/<!! es-reader)
        (log/debug "Closing channel")
        (async/close! channel)
        (log/debug "Waiting for CSV writer to be done")
        (async/<!! csv-writer))
      (s/close! client)
      (log/info "Done")
      (System/exit 0))))

(defn -main
  [& args]
  (main (cli/parse-opts args opts)))
