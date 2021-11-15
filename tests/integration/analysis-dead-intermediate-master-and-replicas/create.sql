-- 22295 replicates from 22294
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where upstream_port=22294;
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22294;
