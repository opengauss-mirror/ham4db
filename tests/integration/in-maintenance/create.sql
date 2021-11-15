INSERT INTO ham_database_instance_maintenance (
    hostname, port, db_type, maintenance_active, begin_timestamp, end_timestamp, owner, reason, processing_node_hostname, processing_node_token, explicitly_bounded
  ) values (
    'testhost', 22293, 'mysql', 1, current_timestamp, current_timestamp, 'test', 'integration test', 'processinghost', 'token', 0
  );
UPDATE ham_database_instance_maintenance SET end_timestamp=current_timestamp + interval 1 minute;
