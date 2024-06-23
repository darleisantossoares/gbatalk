(ns gbatalk.talk
  (:require [datomic.api :as d]
            [clojure.pprint :refer [pprint]]))

(def uri "datomic:dev://localhost:4334/gba")

(d/create-database uri)

(def conn (d/connect uri))

(def schema
  [{:db/ident :pix-transfer-out-request/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :pix-transfer-out-request/requested-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/created-at
    :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/e2e-id
    :db/valueType :db.type/string
    :db/unique :db.unique/value
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/amount
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/message
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/source
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/value}
   {:db/ident :pix-transfer-out-request/scheduled-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/scheduled-to
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/transaction-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/request-hash
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/savings-account
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref
    :db/index true}
   {:db/ident :source/id
    :db/valueType :db.type/uuid
    :db/unique :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident :source/type
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :savings-account/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :savings-account/customer-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}])

@(d/transact conn schema)


(defn create-savings-accounts
  [n]
  (dotimes [_ n]
    @(d/transact conn [{:savings-account/customer-id (random-uuid)
                        :savings-account/id (random-uuid)}])))

;(create-savings-accounts 100000)


(def savings-accounts (d/q '[:find ?sa
                             :where [?sa :savings-account/id]]
                           (d/db conn)))

(def savings-account-ids (map first savings-accounts))

(def pix-transfer-out-request (d/q '[:find (pull ?pix [:pix-transfer-out-request/id])
                             :where [?pix :pix-transfer-out-request/id]]
                           (d/db conn)))

(def pix-transfer-out-request-ids (map first pix-transfer-out-request))

(defn get-random-ptor-ids
  []
  (:pix-transfer-out-request/id (rand-nth pix-transfer-out-request-ids)))

;(println (get-random-ptor-ids))


(defn random-future-date []
  (let [min-days 10
        max-days 100
        random-days (+ min-days (rand-int (- max-days min-days)))
        calendar (java.util.Calendar/getInstance)]
    (.add calendar java.util.Calendar/DAY_OF_YEAR random-days)
    (.getTime calendar)))

(defn create-pix-transfer-out-request
  [source]
  [{:pix-transfer-out-request/id (d/squuid)
    :pix-transfer-out-request/requested-at (new java.util.Date)
    :pix-transfer-out-request/created-at (new java.util.Date)
    :pix-transfer-out-request/amount 10M
    :pix-transfer-out-request/message "testing"
    :pix-transfer-out-request/source source
    :pix-transfer-out-request/savings-account (rand-nth savings-account-ids)
    :pix-transfer-out-request/scheduled-at (new java.util.Date)
    :pix-transfer-out-request/scheduled-to (random-future-date)
    :pix-transfer-out-request/transaction-id (random-uuid)
    :pix-transfer-out-request/request-hash (random-uuid)}])


(defn create-source
  []
  {:source/id (random-uuid)
   :source/type "source-type"})


#_(dotimes [_ 1000000]
  (let [source (create-source)]
    @(d/transact conn (create-pix-transfer-out-request source))))

;(pprint (d/db-stats (d/db conn)))

(defn run
  [{:keys [qtd-pixes]}]
  (dotimes [_ qtd-pixes]
  (let [source (create-source)]
    @(d/transact conn (create-pix-transfer-out-request source)))))



(println (rand-nth savings-account-ids))



(defn get-random-customer-id
  []
  (:savings-account/customer-id (d/entity (d/db conn) (rand-nth savings-account-ids))))

(println (get-random-customer-id))

(pprint (d/query {:query '[:find (pull ?pix [*])
                           :in $
                           :where
                           [?pix :pix-transfer-out-request/scheduled-to  #inst "2024-09-22T00:25:32.476"]
                           [?pix :pix-transfer-out-request/id #uuid "66776b7c-0005-4a34-8ef8-04f6d4709576"]
                           ]
                  :args [(d/db conn)]
                  :io-context :gba-presentation/query-1
                  :query-stats true}))



(pprint (d/query {:query '[:find (pull ?pix [*])
                            :in $
                            :where
                           [?pix :pix-transfer-out-request/id #uuid "66776b7c-0005-4a34-8ef8-04f6d4709576"]
                           [?pix :pix-transfer-out-request/scheduled-to  #inst "2024-09-22T00:25:32.476"]]
                   :args [(d/db conn)]
                   :io-context :gba-presentation/query-2
                   :query-stats true}))



(println "========")

(get-random-ptor-ids)
(println (get-random-customer-id))


(pprint (d/query {:query '[:find ?pix
                           :in $
                           :where
                           [?savings-account :savings-account/customer-id #uuid "b3bfe834-c798-4d71-bb63-bb53eb7df6b7"]
                           [?pix :pix-transfer-out-request/savings-account ?savings-account]]
                  :args [(d/db conn)]
                  :io-context :gba-presentation/query-3
                  :query-stats true}))



(defn get-random-customer-id-with-pix [conn]
  (let [customer-ids (d/q '[:find ?cid
                            :where
                            [?sa :savings-account/customer-id ?cid]
                            [?pix :pix-transfer-out-request/savings-account ?sa]]
                          (d/db conn))]
    (rand-nth (map first customer-ids))))

(defn get-pix-requests-for-random-customer [conn]
  (let [random-customer-id (get-random-customer-id-with-pix conn)]
    (d/query {:query '[:find ?sa ?pix
                :in $ ?cid
                :where
                [?sa :savings-account/customer-id ?cid]
                [?pix :pix-transfer-out-request/savings-account ?sa]]
              :args [ (d/db conn) random-customer-id]
              :io-context :gba-presentation/query-4
              :query-stats true})))

(pprint (get-pix-requests-for-random-customer conn))

