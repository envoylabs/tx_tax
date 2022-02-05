(ns tx-tax.core
  (:require [clojure.data.csv :as csv]
            [jsonista.core :as j]
            [clojure.java.io :as io])
  (:gen-class))

(defn read-json-file [file-path]
  (let [file (io/file file-path)]
    (j/read-value file)))

(defn get-txs [json]
  (get json "txs"))

;; tx utils. these all process one entry in the txs
(defn get-height-from [tx]
  (get tx "height"))

(defn get-tx-hash-from [tx]
  (get tx "txhash"))

(defn get-timestamp-from [tx]
  (get tx "timestamp"))

(defn get-logs-from [tx]
  (get tx "logs"))

;; this assumes the structure of the wasm
;; receieve message
(defn get-received-amount-from [tx]
  "burn is the first message,
   coin_received is the second"
  (try
   (let [logs (get-logs-from tx)
         rcv-msg (-> (first logs)  ;; it's a vec of length 1
                     (get "events")           ;; get events key
                     (nth 1))                 ;; then second event
         amount (-> (get rcv-msg "attributes") ;; get this top level key
                    (nth 3)
                    (get "value")) ;; this needs serious checking but seems right
         ]
     amount)
   (catch Exception e
     "0ujuno")))

(defn process [file-path]
  (let [txs (->> (read-json-file file-path) get-txs)]
    (map (fn [tx]
           {:height (get-height-from tx)
            :tx-hash (get-tx-hash-from tx)
            :timestamp (get-timestamp-from tx)
            :amount (get-received-amount-from tx)})
         txs)))

(defn process-n-pages [n file-root f]
  "Applies a function f to a number of pages n
   assumes you have files in a file-root folder
   named page_x as per the shell script output"
  (let [pages-coll (->> (take n (range))
                        (map inc))]
    (map (fn [page]
           (f (str file-root
                   "/page_"
                   page
                   ".json")))
         pages-coll)))

;; you should have got the pages using the script in the repo.
;; therefore there should be about 100 txs in each
;; any that read less than 100 should be investigated
(defn validate-n-pages [n file-root]
  (let [processed (process-n-pages n file-root process)
        number-processed (count processed)]
    (do
      (println (str "Processed pages: " number-processed))
      (println "Processed in each page:")
      (map-indexed (fn [idx page] (println (str "Page " idx ": " (count page)))) processed))))

(defn cat-n-pages [n file-root]
  "Warning: although this is lazy, eventually it will blow up, of course"
  (let [processed (process-n-pages n file-root process)]
    (reduce into processed)))

(defn processed-json->csv [out-file json-entries-coll]
  "This takes the output of process
   and converts it into a csv"
  (let [header-row ["Height" "Timestamp" "Amount" "Tx"]
        rows (map (fn [{height :height
                       timestamp :timestamp
                       amount :amount
                       tx :tx-hash
                       :as entry}]
                    [height
                     timestamp
                     amount
                     tx])
                  json-entries-coll)]
    (with-open [writer (io/writer out-file)]
      (csv/write-csv writer
                     (cons header-row
                           rows)))))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
