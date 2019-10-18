(ns active-escsv.log
  (:require [taoensso.timbre :as timbre])
  (:import [org.apache.log4j Logger AppenderSkeleton Level]
           [org.apache.log4j.spi LoggingEvent]))

(defmacro log
  [level & args]
  `(timbre/log ~level (event ~@args)))

(defn event
  [& args]
  (apply str (concat ["Event ["]
                     [(clojure.string/join " " args)]
                     ["]"])))

(defn redact
  "Redacts a value in a map."
  [m k]
  (assoc m k "[REDACTED]"))

(defmacro info
  [& args]
  `(timbre/info (event ~@args)))

(defmacro warn
  [& args]
  `(timbre/warn (event ~@args)))

(defmacro error
  [& args]
  `(timbre/error (event ~@args)))

(defmacro fatal
  [& args]
  `(timbre/fatal (event ~@args)))

(defmacro debug
  [& args]
  `(timbre/debug (event ~@args)))

(defmacro trace
  [& args]
  `(timbre/trace (event ~@args)))

(defn- log4j-level->level
  "Convert a log4j level to a keyword suitable here."
  [level]
  (cond
    (= level Level/DEBUG) :debug
    (= level Level/ERROR) :error
    (= level Level/FATAL) :fatal
    (= level Level/INFO) :info
    (= level Level/TRACE) :trace
    (= level Level/WARN) :warn))

(defn- make-log4j-appender
  []
  (proxy [AppenderSkeleton] []
    (append [^LoggingEvent event]
      (log (log4j-level->level (.getLevel event)) (.getMessage event)))
    (close [] nil)
    (requiresLayout [] false)))

(defn redirect-log4j!
  "Redirects log4j output to event logging.

  Removes all other log4j appenders."
  []
  (let [root (Logger/getRootLogger)]
    ;; zap all existing appenders
    (.resetConfiguration (.getLoggerRepository root))
    (.setLevel root Level/INFO)
    (.addAppender root (make-log4j-appender))))

(defn timbre-init!
  [timbre-config-map]
  (timbre/merge-config! timbre-config-map))

(defn init!
  []
  (redirect-log4j!)
  (timbre-init!
   ;; sequentialize printing of log entries by using agents
   {:async? true
    :timestamp-opts {:pattern "yyyy-MM-dd HH:mm:ss.SSS"
                     :locale :jvm-default
                     :timezone :jvm-default}}))

(defn reset-level!
  [level]
  (timbre/set-level! (or level :trace)))
