insert into ham_database_instance_tag (
  hostname, port, db_type, cluster_id, tag_name, tag_value
) values
('testhost', 22293, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','role', 'backup'),
('testhost', 22294, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','role', 'delayed'),
('testhost', 22295, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','role', ''),
('testhost', 22296, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','role', 'backup'),
('testhost', 22297, 'mysql', 'xxxx-yyyy-uuuu-xxxx-yyyy-zzzzz','candidate', '');
