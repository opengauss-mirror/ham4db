UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22293;
UPDATE ham_database_instance SET replication_downstream_lag=60 where port in (22294, 22296, 22297);
