UPDATE mysql_database_instance SET semi_sync_master_enabled=1, semi_sync_master_status=0, semi_sync_master_wait_for_slave_count=2, semi_sync_master_clients=1 where di_port=22293;
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22296;
