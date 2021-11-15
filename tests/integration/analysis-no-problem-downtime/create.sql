UPDATE mysql_database_instance SET slave_sql_running=0 where di_port in (select port from ham_database_instance where upstream_port=22294);
INSERT INTO ham_database_instance_downtime (
    hostname, port, downtime_active, begin_timestamp, end_timestamp, owner, reason
  ) values (
    'testhost', 22295, 1, current_timestamp, current_timestamp, 'test', 'integration test'
  );
UPDATE ham_database_instance_downtime SET end_timestamp=current_timestamp + interval 1 minute;
