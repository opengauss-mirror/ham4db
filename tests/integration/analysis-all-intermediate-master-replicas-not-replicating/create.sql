UPDATE mysql_database_instance SET slave_sql_running=0 where di_port in (select port from ham_database_instance where upstream_port=22294);
