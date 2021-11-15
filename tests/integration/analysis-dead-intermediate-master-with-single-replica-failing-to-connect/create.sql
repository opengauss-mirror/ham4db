-- 22295 replicates from 22294
UPDATE mysql_database_instance SET slave_io_running=0, last_io_error='error connecting to master' where di_port in (select port from ham_database_instance where upstream_port=22294);
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22294;
