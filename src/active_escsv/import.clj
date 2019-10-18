(ns active-escsv.import
  (:require [clojure.core.async :as async]
            [clojure.tools.cli :as cli]
            [qbits.spandex :as s]
            [clj-time.core :as time-core]
            [clj-time.format :as time-format]
            [active-escsv.csv :as csv]
            [active-escsv.elasticsearch :as es]
            [active-escsv.log :as log])
  (:gen-class))

(def opts
  [["-?" "--help" "Show this help about the command line arguments."]
   ["-d" "--delete-index" "Delete data in Elasticsearch first."]
   ["-i" "--index INDEX" "Specify Elasticsearch index (default: panakeia_test)."
    :default "panakeia_test"]
   ["-t" "--type TYPE" "Specify Elasticsearch type (default: event)."
    :default "event"]
   ["-h" "--host HOST" "Specify Elasticsearch host (default: localhost)."
    :default "localhost"]
   ["-p" "--port PORT" "Specify Elasticsearch port (default: localhost)."
    :default "9200"]
   ["-c" "--create-index" "Create and configure the index."]])

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
      (println "Specify all CSV files to import on command line.")
      (println "Files ending with `.csv.gz` are gunzipped on the fly.")
      (println "Files ending with `.csv` are read without gunzip.")
      (println summary)
      (System/exit 0))

    :else
    (let [host-port (str (:host options) ":" (:port options))
          client (s/client {:hosts [host-port]
                            :max-retry-timeout 60000})
          index (:index options)
          type (:type options)
          index-exists? (es/index-exists? client index)]

      (when (and (:delete-index options) index-exists?)
        (es/delete-index client index))

      (when (and (:create-index options) (not index-exists?))
        (es/create-index client index type))

      (doseq [csv-filename arguments]
        (log/info "Importing" csv-filename "to" host-port)
        (let [channel (async/chan)
              [headers csv-reader] (csv/read-csv-loop csv-filename channel)
              es-writer (es/put-data-loop client index type headers channel)]
          (log/debug "Waiting for CSV reader to be done")
          (async/<!! csv-reader)
          (log/debug "Closing channel")
          (async/close! channel)
          (log/debug "Waiting for Elasticsearc writer to be done")
          (async/<!! es-writer)))
      (s/close! client)
      (log/info "Done")
      (System/exit 0))))

(defn -main
  [& args]
  (main (cli/parse-opts args opts)))
