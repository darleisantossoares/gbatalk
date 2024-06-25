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
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-request/succeeded
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :pix-transfer-out-succeeded/id
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/uuid}
   {:db/ident :pix-transfer-out-succeeded/succeeded-at
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/instant}
   {:db/ident :pix-transfer-out-succeeded/pix-transfer-out-request
    :db/cardinality :db.cardinality/one
    :db/valueType :db.type/ref}])

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


#_(dotimes [_ 200000]
  (let [source (create-source)]
    @(d/transact conn (create-pix-transfer-out-request source))))

;(pprint (d/db-stats (d/db conn)))

(defn run
  [{:keys [qtd-pixes]}]
  (dotimes [_ qtd-pixes]
  (let [source (create-source)]
    @(d/transact conn (create-pix-transfer-out-request source)))))


(println (get-random-ptor-ids))

(def random-ptor (get-random-ptor-ids))



(defn get-random-customer-id
  []
  (:savings-account/customer-id (d/entity (d/db conn) (rand-nth savings-account-ids))))

(println (get-random-customer-id))

(pprint (d/query {:query '[:find (pull ?pix [*])
                           :in $
                           :where
                           [?pix :pix-transfer-out-request/scheduled-to  #inst "2024-09-10T16:45:27.471-00:00"]
                           [?pix :pix-transfer-out-request/id #uuid "6679a2a7-1bda-4f36-8b81-02f2a7ff0533"]
                           ]
                  :args [(d/db conn)]
                  :io-context :gba-presentation/query-1
                  :query-stats true}))



(pprint (d/query {:query '[:find (pull ?pix [*])
                            :in $
                            :where
                           [?pix :pix-transfer-out-request/id  #uuid "6679a2a7-1bda-4f36-8b81-02f2a7ff0533"]
                           [?pix :pix-transfer-out-request/scheduled-to   #inst "2024-09-10T16:45:27.471-00:00"]]
                   :args [(d/db conn)]
                   :io-context :gba-presentation/query-2
                   :query-stats true}))



(println "========")

(println (get-random-customer-id))


(pprint (d/query {:query '[:find ?pix
                           :in $
                           :where
                           [?savings-account :savings-account/customer-id #uuid "d6f04b87-cbfe-4ceb-bb08-27c147f11fbf"]
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


 (pprint (let [random-customer-id (get-random-customer-id-with-pix conn)]
          (d/query {:query '[:find ?sa ?pix
                             :in $ ?cid
                             :where
                             [?sa :savings-account/customer-id ?cid]
                             [?pix :pix-transfer-out-request/savings-account ?sa]]
                    :args [(d/db conn) random-customer-id]
                    :io-context :gba-presentation/query-4
                    :query-stats true})))



#_(defn permutations
  [s]
  (println s)
  (if (empty? s)
    '(()))
  (for [x s
        p (permutations (remove #{x} s))]
    (cons x p)))


(pprint (let [random-customer-id (get-random-customer-id-with-pix conn)]
          (d/query {:query '[:find ?sa ?pix
                             :in $ ?cid
                             :where
                             [?pix :pix-transfer-out-request/savings-account ?sa]
                             [?sa :savings-account/customer-id ?cid]]
                    :args [(d/db conn) random-customer-id]
                    :io-context :gba-presentation/query-5
                    :query-stats true})))



;;;;;;;;;;;;;; transactions


(get-random-ptor-ids)

(defn get-pix-transfer-out-request
  []
  (let [db (d/db conn)
        query '[:find ?e
                :where
                [?e :pix-transfer-out-request/id #uuid "66776b8c-53dd-4e94-8fab-d13a623059a5"]]
        result (d/q query db)]
    (ffirst result)))



@(d/transact conn [{:pix-transfer-out-request/id (random-uuid)
                   :pix-transfer-out-request/amount 15M
                   :pix-transfer-out-request/message "new message"}] :io-context :darlei/presentation)








(pprint (d/db-stats (d/db conn)))

