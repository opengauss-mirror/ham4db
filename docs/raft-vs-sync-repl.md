# ham4db/raft vs. synchronous replication setup

This compares deployment, behavioral, limitations and benefits of two high availability deployment approaches: `ham4db/raft` vs `ham4db/[galera|xtradb cluster|innodb cluster]`

We will assume and compare:

- `3` data-centers setup (an _availability zone_ may count as a data-center)
- `3` node `ham4db/raft` setup
- `3` node `ham4db` on multi-writer `galera|xtradb cluster|innodb cluster` (each MySQL in cluster may accept writes)
- A proxy able to run `HTTP` or `mysql` health checks
- `MySQL`, `MariaDB`, `Percona Server` all considered under the term `MySQL`.

![ham4db HA via raft](images/ham4db-ha-raft-vs-sync-repl.png)

| Compare | ham4db/raft | synchronous replication backend |
| --- | --- | --- |
General wiring | Each `ham4db` node has a private backend DB; `ham4db` nodes communicate by `raft` protocol | Each `ham4db` node connects to a different `MySQL` member in a synchronous replication group. `ham4db` nodes do not communicate with each other.
Backend DB | `MySQL` or `SQLite` | `MySQL`
Backend DB dependency | Service panics if cannot access its own private backend DB | Service _unhealthy_ if cannot access its own private backend DB
DB data | Independent across DB backends. May vary, but on a stable system converges to same overall picture | Single dataset, synchronously replicated across DB backends.
DB access | Never write directly. Only `raft` nodes access the backend DB while coordinating/cooperating. Or else inconsistencies can be introduced. Reads are OK. | Possible to access & write directly; all `ham4db` nodes/clients see exact same picture.
Leader and actions | Single leader. Only the leader runs recoveries. All nodes run discoveries (probing) and self-analysis | Single leader. Only the leader runs discoveries (probing), analysis and recoveries.
HTTP Access | Must only access the leader (can be enforced by proxy or `ham4db-client`) | May access any healthy node (can be enforced by proxy). For read consistency always best to speak to leader only (can be enforced by proxy or `ham4db-client`)
Command line | HTTP/API access (e.g. `curl`, `jq`) or `ham4db-client` script which wraps common HTTP /API calls with familiar command line interface | HTTP/API, and/or `ham4db-client` script, or `ham4db ...` command line invocation.
Install | `ham4db` service on service nodes only. `ham4db-client` script anywhere (requires access to HTTP/API). | `ham4db` service on service nodes. `ham4db-client` script anywhere (requires access to HTTP/API). `ham4db` client anywhere (requires access to backend DBs)
Proxy | HTTP. Must only direct traffic to the leader (`/api/leader-check`) | HTTP. Must only direct traffic to healthy nodes (`/api/status`) ; best to only direct traffic to leader node (`/api/leader-check`)
No proxy | Use `ham4db-client` with all `ham4db` backends. `ham4db-client` will direct traffic to master. | Use `ham4db-client` with all `ham4db` backends. `ham4db-client` will direct traffic to master.
Cross DC | Each `ham4db` node (along with private backend) can run on a different DC. Nodes do not communicate much, low traffic. | Each `ham4db` node (along with associated backend) can run on a different DC. `ham4db` nodes do not communicate directly. `MySQL` group replication is chatty. Amount of traffic mostly linear by size of topologies and by polling rate. Write latencies.
Probing | Each topology server probed by all `ham4db` nodes | Each topology server probed by the single active node
Failure analysis | Performed independently by all nodes | Performed by leader only (DB is shared so all nodes see exact same picture anyhow)
Failover | Performed by leader node only | Performed by leader node only
Resiliency to failure | `1` node may go down (`2` on a `5` node cluster) | `1` node may go down (`2` on a `5` node cluster)
Node back from short failure | Node rejoins cluster, gets updated with changes. | DB node rejoins cluster, gets updated with changes.
Node back from long outage | DB must be cloned from healthy node. | Depends on your MySQL backend implementation. Potentially SST/restore from backup.

### Considerations

Here are considerations for choosing between the two approaches:

- You only have a single data center (DC): pick shared DB or even a [simpler setup](high-availability.md)
- You are comfortable with Galera/XtraDB Cluster/InnoDB Cluster and have the automation to set them up and maintain them: pick shared DB backend.
- You have high-latency cross DC network: choose `ham4db/raft`.
- You don't want to allocate MySQL servers for the `ham4db` backend: choose `ham4db/raft` with `SQLite` backend
- You have thousands of MySQL boxes: choose either, but choose `MySQL` backend which is more write performant than `SQLite`.

### Notes

- Another synchronous replication setup is that of a single writer. This would require an additional proxy between the `ham4db` nodes and the underlying cluster, and is not considered above.
