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
      (is (= (couchdb/database-delete +test-db+) true))
      (is (= (- (count db-list) 1)
             (count (couchdb/database-list)))))
    ;; now create the db
    (is (= (couchdb/database-create +test-db+) +test-db+))
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
    (is (zero? (count docs)))
    ;; now create a document with a server-generated ID
    (let [doc (couchdb/document-create +test-db+ {:foo 1})]
      (is (= (:foo (couchdb/document-get +test-db+ (:_id doc))) 1)))
    ;; and recheck the list of documents
    (let [new-docs (couchdb/document-list +test-db+)]
      (is (= 1 (count new-docs))))
    ;; now make a new document with an ID we choose
    (let [new-doc (couchdb/document-create +test-db+ "foobar" {:foo 1})]
      ;; and recheck the list of documents
      (let [new-docs (couchdb/document-list +test-db+)]
        (is (= 2 (count new-docs)))
        (is (= 1 (count (filter #(= % "foobar") new-docs)))))
      ;; and try to get the document back from the server
      (is (= (:foo (couchdb/document-get +test-db+ :foobar)) 1))
      ;; now let's update our document
      (is (= (:foo (couchdb/document-update +test-db+ "foobar" (assoc new-doc :foo 5)) 5)))
      ;; and grab it back from the server just to make sure
      (is (= (:foo (couchdb/document-get +test-db+ :foobar) 5))))))


(deftest cleanup
  ;; be a good citizen and delete the database we use for testing
  (is (= (couchdb/database-delete +test-db+) true)))

(deftest error-checking
  ;; try to access an invalid database name
  (is (raised? couchdb/InvalidDatabaseName (couchdb/database-info "#one")))
  ;; try to get DB that doesn't exist
  (is (raised? couchdb/DatabaseNotFound (couchdb/database-info +test-db+)))
  ;; create our test-db
  (is (= (couchdb/database-create +test-db+) +test-db+))
  ;; try to create it again
  (is (raised? couchdb/PreconditionFailed (couchdb/database-create +test-db+)))
  ;; try to grab non-extant document
  (is (raised? couchdb/DocumentNotFound (couchdb/document-get +test-db+ "foo")))
  ;; create a document with invalid JSON
  (is (raised? couchdb/ServerError (couchdb/document-create +test-db+ "not a JSON object")))
  ;; create a document for reals this time
  (let [doc (couchdb/document-create +test-db+ "foo" {:foo 42})]
    (is (= (:foo doc) 42))
    (is (= (:_id doc) "foo"))
    ;; try to update the document without sending the version
    (is (raised? couchdb/ResourceConflict (couchdb/document-update +test-db+ "foo" {:foo 43})))
    ;; update the document for real
    (couchdb/document-update +test-db+ "foo" (assoc doc :foo 43))
    ;; check that it updated
    (let [new-doc (couchdb/document-get +test-db+ "foo")]
      (is (= (:foo new-doc) 43))))
  ;; create an initial version of a document
  (let [first-rev (couchdb/document-create +test-db+ "bam" {:answer "one"})]
    ;; test that we can just update the document straight up
    (is (= (:answer (couchdb/document-update +test-db+ "bam" (assoc first-rev :answer "two")) "two")))
    ;; now try to insert with the wrong revision
    (is (raised? couchdb/ResourceConflict (couchdb/document-update +test-db+ "bam" (assoc first-rev :answer "three"))))))

(defn test-ns-hook
  []
  (database)
  (document)
  (cleanup)
  (error-checking)
  (cleanup))

(run-tests)
