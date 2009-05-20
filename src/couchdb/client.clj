(ns couchdb.client
  (:require [clojure.contrib [error-kit :as kit]])
  (:use [clojure.contrib.duck-streams :only [read-lines]]
        [clojure.contrib.java-utils :only [as-str]]
        [clojure.contrib.json.read :only [read-json *json-keyword-keys*]]
        [clojure.contrib.json.write :only [json-str]]
        [clojure.http.client :only [request url-encode]]))

(def *server* "http://localhost:5984/")

(kit/deferror InvalidDatabaseName [] [database]
  {:msg (str "Invalid Database Name: " database)
   :unhandled (kit/throw-msg Exception)})

(kit/deferror DatabaseNotFound [] [e]
  {:msg (str "Database Not Found: " e)
   :unhandled (kit/throw-msg java.io.FileNotFoundException)})

(kit/deferror DocumentNotFound [] [e]
  {:msg (str "Document Not Found: " e)
   :unhandled (kit/throw-msg java.io.FileNotFoundException)})

(kit/deferror ResourceConflict [] [e]
  "Raised when a 409 code is returned from the server."
  {:msg (str "Resource Conflict: " e)
   :unhandled (kit/throw-msg Exception)})

(kit/deferror PreconditionFailed [] [e]
  "Raised when a 412 code is returned from the server."
  {:msg (str "Precondition Failed: " e)
   :unhandled (kit/throw-msg Exception)})

(kit/deferror ServerError [] [e]
  "Raised when any unexpected code >= 400 is returned from the server."
  {:msg (str "Unhandled Server Error: " e)
   :unhandled (kit/throw-msg Exception)})


(defn couch-request
  [& args]
  (let [response (apply request args)
        result (try (assoc response :json (binding [*json-keyword-keys* true]
                                            (read-json (apply str
                                                              (:body-seq response)))))
                    (catch Exception e ;; if there's an error reading the JSON, just don't make a :json key
                      response))]
    (if (>= (:code result) 400)
      (kit/raise* ((condp = (:code result)
                     404 (if (= (:reason (:json result)) "Missing") ;;as of svn rev 775577 this should be "no_db_file"
                           DatabaseNotFound
                           DocumentNotFound)
                     409 ResourceConflict
                     412 PreconditionFailed
                     ServerError)
                   {:e (:json result)}))
      result)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;         Utilities           ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn valid-dbname?
  [database]
  (boolean (re-find #"^[a-z][a-z0-9_$()+-/]*$" database)))

(defn validate-dbname
  [database]
  (if (valid-dbname? database)
    (url-encode database)
    (kit/raise InvalidDatabaseName database)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;          Databases          ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn database-list
  []
  (:json (couch-request (str *server* "_all_dbs"))))

(defn database-create
  [database]
  (when-let [database (validate-dbname database)]
    (couch-request (str *server* database) :put)
    database))

(defn database-delete
  [database]
  (when-let [database (validate-dbname database)]
    (couch-request (str *server* database) :delete)
    true))

(defn database-info
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database)))))

(defn database-compact
  [database]
  (when-let [database (validate-dbname database)]
    (couch-request (str *server* database "/_compact") :post)
    true))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;         Documents           ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn document-list
  [database]
  (when-let [database (validate-dbname database)]
    (map :id (:rows (:json (couch-request (str *server* database "/_all_docs")))))))

(defn- do-document-touch
  [database payload id method]
  (when-let [database (validate-dbname database)]
    (let [response (:json (couch-request (str *server* database (when id
                                                                  (str "/" (url-encode (as-str id)))))
                                         method
                                         {"Content-Type" "application/json"}
                                         {}
                                         (json-str payload)))]
      (merge {:_id (:id response), :_rev (:rev response)}
             payload))))

(defn document-create
  ([database payload]
     (do-document-touch database payload nil :post))
  ([database id payload]
     (do-document-touch database payload id :put)))

(defn document-update
  [database id payload]
  ;(assert (:_rev payload)) ;; payload needs to have a revision or you'll get a PreconditionFailed error
  (do-document-touch database payload id :put))

(defn document-get
  ([database id]
     (when-let [database (validate-dbname database)]
       (:json (couch-request (str *server* database "/" (url-encode (as-str id)))))))
  ([database id rev]
     (when-let [database (validate-dbname database)]
       (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "?rev=" rev))))))

(defn document-revisions
  [database id]
  (when-let [database (validate-dbname database)]
    (apply merge
           (reverse (map (fn [m]
                           (apply array-map [(:rev m) (:status m)]))
                         (:_revs_info (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "?revs_info=true")))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;        Attachments          ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn attachment-list
  [database document]
  (let [stringify-top-level-keys (fn [[k v]]
                                   (if (keyword? k)
                                     [(if-let [n (namespace k)]
                                        (str n (name k))
                                        (name k))
                                      v]
                                     [k v]))]
    (into {} (map stringify-top-level-keys
                  (:_attachments (document-get database document))))))

(defn attachment-create
  [database document id payload content-type]
  (when-let [database (validate-dbname database)]
    (let [rev (:_rev (document-get database document))]
      (couch-request (str *server* database "/" (url-encode (as-str document)) "/" id "?rev=" rev)
                     :put
                     {"Content-Type" content-type}
                     {}
                     payload))
    id))

(defn attachment-get
  [database document id]
  (when-let [database (validate-dbname database)]
    (let [response (couch-request (str *server* database "/" (url-encode (as-str document)) "/" id))]
      {:body-seq (:body-seq response)
       :content-type ((:get-header response) "content-type")})))
