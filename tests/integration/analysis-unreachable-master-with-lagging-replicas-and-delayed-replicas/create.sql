UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22293;
UPDATE ham_database_instance SET replication_downstream_lag=600 where port in (22294, 22296, 22297);
UPDATE mysql_database_instance SET sql_delay=600 where di_port in (22297);
