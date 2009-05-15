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


(deftest document
  ;; first get list of documents
  (let [docs (couchdb/document-list +test-db+)]
    (is (zero? (:total_rows docs)))
    (is (= [] (:rows docs)))
    ;; now create a document with a server-generated ID
    (let [doc (couchdb/document-create +test-db+ {:foo 1})]
      (is (= (:ok doc) true))
      (is (= (:foo (couchdb/document-get +test-db+ (:id doc))) 1)))
    ;; and recheck the list of documents
    (let [new-docs (couchdb/document-list +test-db+)]
      (is (= 1 (:total_rows new-docs))))
    ;; now make a new document with an ID we choose
    (is (= (:ok (couchdb/document-create +test-db+ {:foo 1} :foobar)) true))
    ;; and recheck the list of documents
    (let [new-docs (couchdb/document-list +test-db+)]
      (is (= 2 (:total_rows new-docs)))
      (is (= 1 (count (filter #(= (:id %) "foobar") (:rows new-docs))))))
    ;; and try to GET the document back from the server
    (is (= (:foo (couchdb/document-get +test-db+ :foobar)) 1))))



(run-tests)

;; be a good citizen and delete the database we use for testing
(couchdb/database-delete +test-db+)