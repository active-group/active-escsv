(ns active-escsv.elasticsearch
  (:require [clojure.core.async :as async]
            [clojure.set :as set]
            [qbits.spandex :as s]
            [clj-time.core :as time-core]
            [clj-time.format :as time-format]
            [active-escsv.log :as log]))

(def client s/client)
(def close! s/close!)

(defn get-count
  [client index query]
  (log/info "Get count for index" index)
  (let [reply
        (try
          (s/request client {:method :get :url [index "_count"]
                             :body {:query query}})
          (catch Exception reply
            (do
              (log/error "Received errorneous reply" (pr-str reply) (pr-str (ex-data reply)))
              (System/exit 1))))
        cnt
        (get-in reply [:body :count])]
    (log/debug "Received count" (pr-str cnt))
    cnt))

(defn get-headers
  [client index]
  (log/info "Get headers for index" index)
  (let [reply
        (try
          (s/request client {:method :get :url [index "_mapping"]})
          (catch Exception reply
            (do
              (log/error "Received errorneous reply" (pr-str reply) (pr-str (ex-data reply)))
              (System/exit 1))))
        headers
        (reduce-kv (fn [st k v]
                     (set/union st (keys (get-in v [:mappings :properties]))))
                   #{} (:body reply))]
    (log/debug "Received headers " (pr-str headers))
    headers))

(defn get-data-loop
  [client index query es-count to-csv-chan]
  (log/info "Starting reading Elasticsearch loop.")
  (async/go
    (let [ch (s/scroll-chan client {:method :get :url [index "_search"]
                                    :body
                                    {:size 10000
                                     :query query}})]
      (loop [total-count 0]
        (log/debug "Receiving hits (" total-count "/" es-count ")")
        (when-let [reply (async/<! ch)]
          (if (instance? Exception reply)
            (do
              (log/error "Received errorneous reply" (pr-str reply) (pr-str (ex-data reply))))
            (let [current-hits (map :_source (get-in reply [:body :hits :hits]))
                  cnt (count current-hits)
                  new-total-count (+ total-count cnt)]
              (log/debug "Received" cnt "hits (" new-total-count "/" es-count ")")
              (async/>!! to-csv-chan current-hits)
              (recur new-total-count))))))
    (log/info "Done reading Elasticsearch loop.")))

(defn delete-index
  [client index]
  (log/info "Deleting index" index)
  (s/request client {:method :delete :url [index]})
  (log/info "Deleted index" index))

(defn bulkize-data
  [index type data]
  (mapcat (fn [d]
            [{:index {:_index index :_type type}}
             (-> d
                 (dissoc "_index")
                 (dissoc "_type")
                 (dissoc "_id"))])
          data))

(def bulk-chan s/bulk-chan)

(defn index-exists?
  [client index]
  (log/info "Check if index" index "exists")
  (let [res (= 200
               (:status
                (try
                  (s/request client {:method :head :url [index] :include_type_name true})
                  (catch Exception e
                    (log/error "Checking if index exists failed:" (pr-str e))))))]
    (if res
      (log/info "Index" index "exists.")
      (log/info "Index" index "does not exist."))
    res))

(defn create-index
  [client index type]
  (try
    (let [res (s/request client {:method :put :url [index]
                                 :body {:include_type_name false
                                        :mappings
                                        {type
                                         {:properties
                                          {"@timestamp"
                                           {:type :date}}}}}})]
      (log/info (str "CREATED index " index " and SET @timestamp type")))
    (catch Exception res
      (when (instance? Exception res)
        (log/error "Failed to create index" (pr-str (ex-data res)))))))

(defn put-data-loop
  [client index type headers to-elasticsearch-chan]
  (log/info "Starting writing Elasticsearch loop.")
  (async/go
    (let [{:keys [input-ch output-ch]} (s/bulk-chan client
                                                    {:flush-threshold 1000
                                                     :flush-interval 1000
                                                     :max-concurrent-requests 3})]
      (loop [total-count 0]
        (log/debug "Receiving rows (" total-count ")")
        (when-let [data (async/<! to-elasticsearch-chan)]
          (let [cnt (count data)
                new-total-count (+ total-count cnt)]
            (log/debug "Received" cnt "rows (" new-total-count ")")
            (log/debug "Writing" cnt "rows (" new-total-count ") to Elasticsearch")
            (async/>! input-ch (bulkize-data index type data))
            (let [[job responses] (async/<! output-ch)
                  errors? (get-in responses [:body :errors])]
              (if errors?
                (log/error "Failed to write (" new-total-count ") to Elasticsearch: "
                           (pr-str (remove nil? (map #(get-in % [:index :error]) (get-in responses [:body :items])))))
                (log/debug "Wrote" cnt "rows (" new-total-count ") to Elasticsearch")))
            (recur new-total-count)))))
    (log/info "Done writing Elasticsearch loop.")))
