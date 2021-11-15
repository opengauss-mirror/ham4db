UPDATE ham_database_instance SET
  last_checked_timestamp=current_timestamp,
  last_attempted_check_timestamp=current_timestamp,
  last_seen_timestamp=current_timestamp,
  replication_downstream_lag=7
where port in (22294, 22297);

UPDATE mysql_database_instance SET
  seconds_behind_master=7
where di_port in (22294, 22297);
