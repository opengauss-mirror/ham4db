UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22293;
UPDATE ham_database_instance SET is_last_check_partial_success = 1 where port=22293;
