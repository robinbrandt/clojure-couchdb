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

(kit/deferror AttachmentNotFound [] [e]
  {:msg (str "Attachment Not Found: " e)
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
                     404 (condp = (:reason (:json result))
                           "Missing" DatabaseNotFound ;; as of svn rev 775577 this should be "no_db_file"
                           "Document is missing attachment" AttachmentNotFound
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

(defn stringify-top-level-keys
  [[k v]]
  (if (keyword? k)
    [(if-let [n (namespace k)]
       (str n (name k))
       (name k))
     v]
    [k v]))


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
(declare document-get)

(defn- do-get-doc
  [database document]
  (if (map? document)
    (if-let [id (:_id document)]
      id
      (kit/raise ResourceConflict "missing :_id key"))
    document))

(defn- do-get-rev
  [database document]
  (if (map? document)
    (if-let [rev (:_rev document)]
      rev
      (kit/raise ResourceConflict "missing :_rev key"))
    (:_rev (document-get database document))))

(defn- do-document-touch
  [database payload id method]
  (when-let [database (validate-dbname database)]
    (let [response (:json (couch-request (str *server* database (when id
                                                                  (str "/" (url-encode (as-str id)))))
                                         method
                                         {"Content-Type" "application/json"}
                                         {}
                                         (json-str payload)))]
      (merge payload
             {:_id (:id response)
              :_rev (:rev response)}))))

(defn document-list
  ([database]
     (when-let [database (validate-dbname database)]
       (map :id (:rows (:json (couch-request (str *server* database "/_all_docs")))))))
  ([database options]
     (when-let [database (validate-dbname database)]
       (map :id (:rows (:json (couch-request (str *server* database "/_all_docs?" (url-encode options)))))))))

(defn document-create
  ([database payload]
     (do-document-touch database payload nil :post))
  ([database id payload]
     (do-document-touch database payload id :put)))

(defn document-update
  [database id payload]
  ;(assert (:_rev payload)) ;; payload needs to have a revision or you'll get a PreconditionFailed error
  (let [id (do-get-doc database id)]
    (do-document-touch database payload id :put)))

(defn document-get
  ([database id]
     (when-let [database (validate-dbname database)]
       (let [id (do-get-doc database id)]
         (:json (couch-request (str *server* database "/" (url-encode (as-str id))))))))
  ([database id rev]
     (when-let [database (validate-dbname database)]
       (let [id (do-get-doc database id)]
         (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "?rev=" rev)))))))

(defn document-delete
  [database id]
  (when-let [database (validate-dbname database)]
    (let [id (do-get-doc database id)
          rev (do-get-rev database id)]
      (couch-request (str *server* database "/" (url-encode (as-str id)) "?rev=" rev)
                     :delete)
      true)))

(defn- revision-comparator
  [x y]
  (> (Integer/decode (apply str (take-while #(not= % \-) x)))
     (Integer/decode (apply str (take-while #(not= % \-) y)))))

(defn document-revisions
  [database id]
  (when-let [database (validate-dbname database)]
    (let [id (do-get-doc database id)]
      (apply merge (map (fn [m]
                          (sorted-map-by revision-comparator (:rev m) (:status m)))
                        (:_revs_info (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "?revs_info=true")))))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;        Attachments          ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn attachment-list
  [database document]
  (let [document (do-get-doc database document)]
    (into {} (map stringify-top-level-keys
                  (:_attachments (document-get database document))))))


(defn attachment-create
  [database document id payload content-type]
  (when-let [database (validate-dbname database)]
    (let [document (do-get-doc database document)
          rev (do-get-rev database document)]
      (couch-request (str *server* database "/" (url-encode (as-str document)) "/" (url-encode (as-str id)) "?rev=" rev)
                     :put
                     {"Content-Type" content-type}
                     {}
                     payload))
    id))

(defn attachment-get
  [database document id]
  (when-let [database (validate-dbname database)]
    (let [document (do-get-doc database document)
          response (couch-request (str *server* database "/" (url-encode (as-str document)) "/" (url-encode (as-str id))))]
      {:body-seq (:body-seq response)
       :content-type ((:get-header response) "content-type")})))

(defn attachment-delete
  [database document id]
  (when-let [database (validate-dbname database)]
    (let [document (do-get-doc database document)
          rev (do-get-rev database document)]
      (couch-request (str *server* database "/" (url-encode (as-str document)) "/" (url-encode (as-str id)) "?rev=" rev)
                     :delete)
      true)))
