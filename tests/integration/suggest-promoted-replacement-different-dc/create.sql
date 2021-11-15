UPDATE ham_database_instance SET upstream_port=22294 where port not in (22293, 22294);
-- topology:
--
-- 22293
-- + 22294
--   + 22295
--   + 22296
--   + 22297
--
UPDATE ham_database_instance SET pl_data_center='seattle', environment='prod';
UPDATE ham_database_instance SET pl_data_center='ny', environment='prod' where port in (22293, 22296);

INSERT INTO ham_database_instance_candidate (hostname, port, db_type, cluster_id, promotion_rule) VALUES ('testhost', 22295, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','prefer');
