(ns couchdb.client
  (:require [clojure.contrib [error-kit :as kit]])
  (:use [clojure.contrib.java-utils :only [as-str]]
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
        result (assoc response :json (binding [*json-keyword-keys* true]
                                       (read-json (apply str
                                                         (:body-seq response)))))]
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
;;          Database           ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn database-list
  []
  (:json (couch-request (str *server* "_all_dbs/"))))

(defn database-create
  [database]
  (when-let [database (validate-dbname database)]
    (couch-request (str *server* database "/") :put)
    database))

(defn database-delete
  [database]
  (when-let [database (validate-dbname database)]
    (couch-request (str *server* database "/") :delete)
    true))

(defn database-info
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/")))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;          Document           ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn document-list
  [database]
  (when-let [database (validate-dbname database)]
    (map :id (:rows (:json (couch-request (str *server* database "/_all_docs/")))))))

(defn- do-document-touch
  [database payload id method]
  (when-let [database (validate-dbname database)]
    (let [response (:json (couch-request (str *server* database "/" (when id
                                                                      (str (url-encode (as-str id)) "/")))
                                         method
                                         {"Content-Type" "application/json"}
                                         {}
                                         (json-str payload)))]
      (merge {:_id (:id response), :_rev (:rev response)}
             payload))))

(defn document-create
  ([database payload]
     (do-document-touch database payload nil :post))
  ([database payload id]
     (do-document-touch database payload id :put)))

(defn document-update
  [database payload id]
  (do-document-touch database payload id :put))

(defn document-get
  [database id]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "/")))))
