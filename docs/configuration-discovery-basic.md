# Configuration: basic discovery

Let ham4db know how to query the topologies, what information to extract.

```json
{
  "MySQLTopologyCredentialsConfigFile": "/etc/mysql/ham4db-topology.cnf",
  "InstancePollSeconds": 5,
  "DiscoverByShowSlaveHosts": false,
}
```

`MySQLTopologyCredentialsConfigFile` follows similar rules as `MySQLCredentialsConfigFile`. You may choose to use plaintext credentials:

```
[client]
user=ham4db
password=orc_topology_password
```

Or, you may choose to use plaintext credentials:

```json
{
  "MySQLTopologyUser": "ham4db",
  "MySQLTopologyPassword": "orc_topology_password",
}
```

`ham4db` will probe each server once per `InstancePollSeconds` seconds.

On all your MySQL topologies, grant the following:

```
CREATE USER 'ham4db'@'orc_host' IDENTIFIED BY 'orc_topology_password';
GRANT SUPER, PROCESS, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'ham4db'@'orc_host';
GRANT SELECT ON meta.* TO 'ham4db'@'orc_host';
GRANT SELECT ON ndbinfo.processes TO 'ham4db'@'orc_host'; -- Only for NDB Cluster
GRANT SELECT ON performance_schema.replication_group_members TO 'ham4db'@'orc_host'; -- Only for Group Replication / InnoDB cluster
```
