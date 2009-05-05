(ns clojure-couchdb
  (:require (couchdb [client :as couchdb]))
  (:use (clojure.contrib [test-is :as test-is])))

(def +test-db+ "clojure-couchdb-test-database")


(deftest database
  ;; get a list of existing DBs
  (let [db-list (couchdb/database-list)
        has-test-db? (some #{+test-db+} db-list)]
    ;; if the db exists, delete it
    (when has-test-db?
      (is (= (:ok (couchdb/database-delete +test-db+)) true))
      (is (= (- (count db-list) 1)
             (count (couchdb/database-list)))))
    ;; now create the db
    (is (= (:ok (couchdb/database-create +test-db+)) true))
    (if has-test-db?
      (is (= (count db-list)
             (count (couchdb/database-list))))
      (is (= (+ (count db-list) 1)
             (count (couchdb/database-list))))))
  ;; now get info about the db
  (let [info (couchdb/database-info +test-db+)]
    (is (= (:db_name info) +test-db+))
    (is (= (:doc_count info) 0))
    (is (= (:doc_del_count info) 0))
    (is (= (:update_seq info) 0))))




(run-tests)

;; be a good citizen and delete the database we use for testing
(couchdb/database-delete +test-db+)