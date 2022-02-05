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
  (-> (get tx "height")
      Integer/parseInt))

(defn get-tx-hash-from [tx]
  (get tx "txhash"))

(defn get-timestamp-from [tx]
  (get tx "timestamp"))

(defn get-logs-from [tx]
  (get tx "logs"))

(defn get-rcv-msgs-from [logs]
  (->> (map #(get % "events") logs)
       (map (fn [events]
              (filter #(= (get % "type")
                          "coin_received")
                      events)))
       (reduce into)))

(defn get-rcv-msgs-from-tx-for [acct tx]
  (try
    (let [logs (get-logs-from tx)
          rcv-msgs (->> (get-rcv-msgs-from logs)
                        (map #(get % "attributes"))) ;; drop down into msgs & flatten
          indexed-tuples (->> rcv-msgs
                              (reduce into)
                              (map-indexed (fn [idx m] [idx m])))
          idxs (->> indexed-tuples ;; get where value is the acct
                    (filter #(= acct
                                (-> (second %)
                                    (get "value"))))
                    (map first))
          get-amount-by-idx (fn [idx] (-> (nth indexed-tuples
                                              (inc idx)) ;; the next entry will be the amount
                                         second ;; get the hashmap not idx
                                         (get "value")))]
      (map get-amount-by-idx idxs))
    (catch Exception e
      ["0ujuno"])))

;; this assumes the structure of the wasm
;; receieve message
(defn get-received-amount-from [tx]
  "burn is the first message,
   coin_received is the second"
  (try
   (let [logs (get-logs-from tx)
         rcv-msgs (get-rcv-msgs-from logs)
         amounts (->> (map #(get % "attributes") rcv-msgs)
                      (filter #(= (get % "key") "amount")) ;; get this top level key
                      (map #(get % "value"))) ]
     amounts)
   (catch Exception e
     "0ujuno")))

(defn process [file-path]
  "This assumes a particular file structure"
  (let [txs (->> (read-json-file file-path) get-txs)]
    (map (fn [tx]
           {:height (get-height-from tx)
            :tx-hash (get-tx-hash-from tx)
            :timestamp (get-timestamp-from tx)
            :amount (get-received-amount-from tx)})
         txs)))

(defn process-multi [acct file-path]
  "This assumes a number of events in the log
   and that it will be looking up the relevant
   ones using the provided address"
  (let [txs (->> (read-json-file file-path) get-txs)]
    (->> (map (fn [tx]
                (let [amounts (get-rcv-msgs-from-tx-for acct tx)]
                  (map (fn [amt] {:height (get-height-from tx)
                                 :tx-hash (get-tx-hash-from tx)
                                 :timestamp (get-timestamp-from tx)
                                 :amount amt})
                       amounts)))
              txs)
         (reduce into))))
;; example usage
(comment
  (->> (cat-n-pages 49
                    "./data/whoami"
                    (partial process-multi "juno1t4l87r4zvyp2p24dscet4e6atjf28r98yeq2h3"))
       (sort-by :height)
       (processed-json->csv "./output/received.csv")))


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

(defn cat-n-pages [n file-root f]
  "Warning: although this is lazy, eventually it will blow up, of course
   Takes a function f to use when processing"
  (let [processed (process-n-pages n file-root f)]
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

;; for testing
(comment
  (def test-tx (nth (->> (read-json-file "./test_data/test.json") get-txs) 1))
  (def test-tx-2 (nth (->> (read-json-file "./test_data/validator_test.json") get-txs) 2)))
