# Ham4DB High Availability

`ham4db` runs as a highly available service. This document lists the various ways of achieving HA for `ham4db`, as well as less/not highly available setups.

### TL;DR ways to get HA

HA is achieved by choosing either:

- `ham4db/raft` setup, where `ham4db` nodes communicate by raft consensus. Each `ham4db` node [has a private database backend](#ha-via-raft), either `MySQL` or `sqlite`. See also [ham4db/raft documentation](raft.md)
- [Shared backend](#ha-via-shared-backend) setup. Multiple `ham4db` nodes all talk to the same backend, which may be a Galera/XtraDB Cluster/InnoDB Cluster/NDB Cluster. Synchronization is done at the database level.

See also: [ham4db/raft vs. synchronous replication setup](raft-vs-sync-repl.md)

### Availability types

You may choose different availability types, based on your requirements.

- No high availability: easiest, simplest setup, good for testing or dev setups. Can use `MySQL` or `sqlite`
- Semi HA: backend is based on normal MySQL replication. `ham4db` does not eat its own dog food and cannot failover its own backend.
- HA: as depicted above; support for no single point of failure. Different solutions have different tradeoff in terms of resource utilization, supported software, type of client access.

Discussed below are all options.

### No high availability

![ham4db no HA](images/ham4db-ha--no-ha.png)

This setup is good for CI testing, for local dev machines or otherwise experiments. It is a single-`ham4db` node with a single DB backend.

The DB backend may be a `MySQL` server or it may be a `sqlite` DB, bundled with `ham4db` (no dependencies, no additional software required)

### Semi HA

![ham4db semi HA](images/ham4db-ha--semi-ha.png)

This setup provides semi HA for `ham4db`. Two variations available:

- Multiple `ham4db` nodes talk to the same backend database. HA of the `ham4db` services is achieved. However HA of the backend database is not achieved. Backend database may be a `master` with replicas, but `ham4db` is unable to eat its own dog food and failover its very backend DB.

  If the backend `master` dies, it takes someone or something else to failover the `ham4db` service onto a promoted replica.

- Multiple `ham4db` services all talk to a proxy server, which load balances an active-active `MySQL` master-master setup with `STATEMENT` based replication.

  - The proxy always directs to same server (e.g. `first` algorithm for `HAProxy`) unless that server is dead.
  - Death of the active master causes `ham4db` to talk to other master, which may be somewhat behind. `ham4db` will typically self reapply the missing changes by nature of its continuous discovery.
  - `ham4db` queries guarantee `STATEMENT` based replication will not cause duplicate errors, and master-master setup will always achieve consistency.
  - `ham4db` will be able to recover the death of a backend master even if in the middle of runnign a recovery (recovery will re-initiate on alternate master)
  - **Split brain is possible**. Depending on your setup, physical locations, type of proxy, there can be different `ham4db` service nodes speaking to different backend `MySQL` servers. This scenario can lead two two `ham4db` services which consider themselves as "active", both of which will run failovers independently, which would lead to topology corruption.

To access your `ham4db` service you may speak to any healthy node.

Both these setups are well known to run in production for very large environments.

### HA via shared backend

![ham4db HA via shared backend](images/ham4db-ha--shared-backend.png)

HA is achieved by highly available shared backend. Existing solutions are:

- Galera
- XtraDB Cluster
- InnoDB Cluster
- NDB Cluster

In all of the above the MySQL nodes run synchronous replication (using the common terminology).

Two variations exist:

- Your Galera/XtraDB Cluster/InnoDB Cluster runs with a single-writer node. Multiple `ham4db` nodes will speak to the single writer DB, probably via proxy. If the writer DB fails, the backend cluster promotes a different DB as writer; it is up to your proxy to identify that and direct `ham4db`'s traffic to the promoted server.

- Your Galera/XtraDB Cluster/InnoDB Cluster runs in multiple writers mode. A nice setup would couple each `ham4db` node with a DB server (possibly on the very same box). Since replication is synchronous there is no split brain. Only one `ham4db` node can ever be the leader, and that leader will only speak with a consensus of the DB nodes.

In this setup there could be a substantial amount of traffic between the MySQL nodes. In cross-DC setups this may imply larger commit latencies (each commit may need to travel cross DC).

To access your `ham4db` service you may speak to any healthy node. It is advisable you speak only to the leader via proxy (use `/api/leader-check` as HTTP health check for your proxy).

The latter setup is known to run in production at a very large environment on `3` or `5` nodes setup.

### HA via raft

![ham4db HA via raft](images/ham4db-ha--raft.png)

`ham4db` nodes will directly communicate via `raft` consensus algorithm. Each `ham4db` node has its own private backend database. This can be `MySQL` or `sqlite`.

Only one `ham4db` node assumes leadership, and is always a part of a consensus. However all other nodes are independently active and are polling your topologies.

In this setup there is:
- No communication between the DB nodes.
- Minimal communication between the `ham4db`.
- `*n` communication to `MySQL` topology nodes. A `3` node setup means each topology `MySQL` server is probed by `3` different `ham4db` nodes, independently.

It is recommended to run a `3`-node or a `5`-node setup.

`sqlite` is embedded within `ham4db` and does not require an external dependency. `MySQL` outperforms `sqlite` on busy setups.

To access your `ham4db` service you may **only** speak to the leader node.
- Use `/api/leader-check` as HTTP health check for your proxy.
- Or use [ham4db-client](client.md) with multiple `ham4db` backends; `ham4db-client` will figure out the identity of the leader and will send requests to the leader.

![ham4db HA via raft](images/ham4db-ha--raft-proxy.png)

`ham4db/raft` is a newer development, and is being tested in production at this time. Please read the [ham4db/raft documentation](raft.md) for all implications.
