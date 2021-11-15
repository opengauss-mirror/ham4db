UPDATE mysql_database_instance SET slave_sql_running=0 where di_port in (select port from ham_database_instance where upstream_port=22293);
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port in (22296, 22297);
