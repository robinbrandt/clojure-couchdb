(ns couchdb.client
  (:require (org.danlarkin [json :as json]))
  (:use [clojure.http.client :only [request url-encode]]))

(def *server* "http://localhost:5984/")

(defn couch-request
  [& args]
  (let [result (apply request args)]
    (assoc result :json (json/decode-from-str (apply str
                                                     (:body-seq result))))))

(defn valid-dbname?
  [name]
  (boolean (re-find #"^[a-z][a-z0-9_$()+-/]*$" name)))

(defn validate-dbname
  [name]
  (when (valid-dbname? name)
    (url-encode name)))

(defn database-list
  []
  (:json (couch-request (str *server* "_all_dbs/"))))

(defn database-create
  [name]
  (when-let [name (validate-dbname name)]
    (:json (couch-request (str *server* name "/") :put))))
    
(defn database-delete
  [name]
  (when-let [name (validate-dbname name)]
    (:json (couch-request (str *server* name "/") :delete))))

(defn database-info
  [name]
  (when-let [name (validate-dbname name)]
    (:json (couch-request (str *server* name "/")))))
