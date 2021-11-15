UPDATE mysql_database_instance SET semi_sync_master_enabled=1, semi_sync_master_status=1, semi_sync_master_wait_for_slave_count=2, semi_sync_master_clients=1 where di_port=22293;
UPDATE ham_database_instance SET last_seen_timestamp=last_checked_timestamp - interval 1 minute where port=22296;
INSERT INTO mysql_database_instance_stale_binlog_coordinate (
    hostname, port,	binary_log_file, binary_log_pos, first_seen_timestamp
  )
  values (
    'testhost', 22293, 'mysql-bin.000167', 137086726, CURRENT_TIMESTAMP
  );
UPDATE mysql_database_instance_stale_binlog_coordinate SET first_seen_timestamp=current_timestamp - interval 1 minute;
