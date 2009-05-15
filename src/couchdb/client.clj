(ns couchdb.client
  (:use [clojure.contrib.java-utils :only [as-str]]
        [clojure.contrib.json.read :only [read-json *json-keyword-keys*]]
        [clojure.contrib.json.write :only [json-str]]
        [clojure.http.client :only [request url-encode]]))

(def *server* "http://localhost:5984/")

(defn couch-request
  [& args]
  (let [result (apply request args)]
    (assoc result :json (binding [*json-keyword-keys* true]
                          (read-json (apply str
                                            (:body-seq result)))))))

(defn valid-dbname?
  [database]
  (boolean (re-find #"^[a-z][a-z0-9_$()+-/]*$" database)))

(defn validate-dbname
  [database]
  (when (valid-dbname? database)
    (url-encode database)))

(defn database-list
  []
  (:json (couch-request (str *server* "_all_dbs/"))))

(defn database-create
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/") :put))))

(defn database-delete
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/") :delete))))

(defn database-info
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/")))))




(defn document-list
  [database]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/_all_docs/")))))

(defn document-create
  ([database payload]
   (when-let [database (validate-dbname database)]
     (:json (couch-request (str *server* database "/")
                           :post
                           {"Content-Type" "application/json"}
                           {}
                           (json-str payload)))))
  ([database payload id]
   (when-let [database (validate-dbname database)]
     (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "/")
                           :put
                           {"Content-Type" "application/json"}
                           {}
                           (json-str payload))))))

(defn document-get
  [database id]
  (when-let [database (validate-dbname database)]
    (:json (couch-request (str *server* database "/" (url-encode (as-str id)) "/")))))
