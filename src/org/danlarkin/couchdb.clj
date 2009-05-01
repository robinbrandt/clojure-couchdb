(ns org.danlarkin.couchdb
  (:require (org.danlarkin [json :as json]))
  (:use [clojure.http.client :only [request]]))

(def *server* "http://localhost:5984/")

(defn couch-request
  [& args]
  (let [result (apply request args)]
    (assoc result :json (json/decode-from-str (apply str
                                                     (:body-seq result))))))

(defn valid-dbname?
  [name]
  (boolean (re-find #"^[a-z0-9_$()+-/]+$" name)))

(defn database-list
  []
  (:json (couch-request (str *server* "_all_dbs/"))))

(defn database-create
  [name]
  (when (valid-dbname? name)
    (couch-request (str *server* name "/") :put)))
    

(prn (database-list))
;(prn (database-create "ok"))
