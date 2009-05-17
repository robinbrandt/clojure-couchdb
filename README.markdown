#clojure-couchdb

##Database Functions:

###database-list
    user=> (database-list)
    ["foo" "bar" "some-db" "another-database"]
###database-info
    user=> (database-info "foo")
    {:instance_start_time "1242496720047538", :disk_size 4096, :compact_running false, :purge_seq 0, :update_seq 0, :doc_del_count 0, :doc_count 0, :db_name "foo"}
###database-create
    user=> (database-create "new-db")
    "new-db"
###database-delete
    user=> (database-delete "new-db")
    true

##Document Functions:

###document-list
    user=> (document-list "some-db")
    ("doc1" "doc2" "another-doc" "yet-another-doc")
###document-get
    user=> (document-get "some-db" "doc1")
    {:bar [1 2 3], :_rev "1-2326402976", :_id "doc1"}
###document-create
    user=> (document-create "some-db" {:foo 42})
    {:foo 42, :_id "5bc3673322f38a4998aca23976acd4c6", :_rev "1-1799037045"}
    user=> (document-create "some-db" {:foo 42} "my-doc")
    {:foo 42, :_id "my-doc", :_rev "1-2310851567"}
###document-update
    user=> (let [doc (document-get "some-db" "my-doc")]
             (document-update "some-db" (assoc doc :bam true) "my-doc"))
    {:foo 42, :bam true, :_id "my-doc", :_rev "1-2310851567"}
