# Ham4db/raft, consensus cluster

![ham4db HA via raft](images/ham4db-ha--raft.png)

`ham4db/raft` is a deployment setup where several `ham4db` nodes communicate with each other via `raft` consensus protocol.

`ham4db/raft` deployments solve both high-availability for `ham4db` itself as well as solve issues with network isolation, and in particular cross-data-center network partitioning/fencing.

### Very brief overview of traits of raft

By using a consensus protocol the `ham4db` nodes are able to pick a leader that has _quorum_, implying it is not isolated. For example, consider a `3` node `ham4db/raft` setup. Normally the three nodes will chat with each other and one will be a stable elected leader. However in face of network partitioning, say node `n1` is partitioned away from nodes `n2` and `n3`, it is guaranteed that the leader will be either `n2` or `n3`. `n1` would not be able to lead because it does not have a quorum (in a `3` node setup the quorum size is `2`; in a `5` node setup the quorum size is `3`)

This turns useful in cross data-center (DC) setups. Assume you set three `ham4db` nodes, each on its own DC. If one DC gets isolated, it is guaranteed the active `ham4db` node will be one that has consensus, i.e. operates from outside the isolated DC.

### ham4db/raft setup technical details

See also: [ham4db/raft vs. synchronous replication setup](raft-vs-sync-repl.md)

#### Service nodes

You will set up `3` or `5` (recommended raft node count) `ham4db` nodes. Other numbers are also legitimate but you will want at least `3`.

At this time `ham4db` nodes to not join dynamically into the cluster. The list of nodes is preconfigured as in:

```json
  "RaftEnabled": true,
  "RaftDataDir": "/var/lib/ham4db",
  "RaftBind": "<ip.or.fqdn.of.this.ham4db.node>",
  "DefaultRaftPort": 10008,
  "RaftNodes": [
    "<ip.or.fqdn.of.ham4db.node1>",
    "<ip.or.fqdn.of.ham4db.node2>",
    "<ip.or.fqdn.of.ham4db.node3>"
  ],
```

#### Backend DB

Each `ham4db` node has its own, dedicated backend database server. This would be either:

- A MySQL backend DB (no replication setup required, but OK if this server has replicas)

  As deployment suggestion, this MySQL server can run on the same `ham4db` node host.

- A SQLite backend DB. Use:
```json
  "BackendDB": "sqlite",
  "SQLite3DataFile": "/var/lib/ham4db/ham4db.db",
```

`ham4db` is bundled with `sqlite`, there is no need to install an external dependency.

#### Proxy: leader

Only the leader is allowed to make changes.

Simplest setup it to only route traffic to the leader, by setting up a `HTTP` proxy (e.g HAProxy) on top of the `ham4db` services.

> See [ham4db-client](#ham4db-client) section for an alternate approach

- Use `/api/leader-check` as health check. At any given time at most one `ham4db` node will reply with `HTTP 200/OK` to this check; the others will respond with `HTTP 404/Not found`.
  - Hint: you may use, for example, `/api/leader-check/503` is you explicitly wish to get a `503` response code, or similarly any other code.
- Only direct traffic to the node that passes this test

As example, this would be a `HAProxy` setup:

```
listen ham4db
  bind  0.0.0.0:80 process 1
  bind  0.0.0.0:80 process 2
  bind  0.0.0.0:80 process 3
  bind  0.0.0.0:80 process 4
  mode tcp
  option httpchk GET /api/leader-check
  maxconn 20000
  balance first
  retries 1
  timeout connect 1000
  timeout check 300
  timeout server 30s
  timeout client 30s

  default-server port 3000 fall 1 inter 1000 rise 1 downinter 1000 on-marked-down shutdown-sessions weight 10

  server ham4db-node-0 ham4db-node-0.fqdn.com:3000 check
  server ham4db-node-1 ham4db-node-1.fqdn.com:3000 check
  server ham4db-node-2 ham4db-node-2.fqdn.com:3000 check
```

#### Proxy: healthy raft nodes

A relaxation of the above constraint.

Healthy raft nodes will reverse proxy your requests to the leader. You may choose (and this happens to be desirable for `kubernetes` setups) to talk to any healthy raft member.

You _must not access unhealthy raft members, i.e. nodes that are isolated from the quorum_.

- Use `/api/raft-health` to identify that a node is part of a healthy raft group.
- A `HTTP 200/OK` response identifies the node as part of the healthy group, and you may direct traffic to the node.
- A `HTTP 500/Internal Server Error` indicates the node is not part of a healthy group.
  Note that immediately following startup, and until a leader is elected, you may expect some time where all nodes report as unhealthy.
  Note that upon leader re-election you may observe a brief period where all nodes report as unhealthy.

#### ham4db-client

An alternative to the proxy approach is to use `ham4db-client`.

[ham4db-client](client.md) is a wrapper script that accesses the `ham4db` service via HTTP API, and provides a command line interface to the user.

It is possible to provide `ham4db-client` with the full listing of all ham4db API endpoints. In such case, `ham4db-client` will figure out which of the endpoints is the leader, and direct requests at that endpoint.

As example, we can set:

```shell
export HAM4DB_API="https://ham4db.host1:3000/api https://ham4db.host2:3000/api https://ham4db.host3:3000/api"
```

A call to `ham4db-client` will first check

Otherwise, if you already have a proxy, it's also possible for `ham4db-client` to work with the proxy, e.g.:

```shell
export HAM4DB_API="https://ham4db.proxy:80/api"
```

### Behavior and implications of ham4db/raft setup

- In the `raft` setup, each `ham4db` node independently runs discoveries of all servers. This means that in a three node setup, each of your `MySQL` topology servers will be independently visited by three different `ham4db` nodes.

- In normal times, the three nodes will see a more-or-less identical picture of the topologies. But they will each have their own independent analysis.

- Each `ham4db` nodes writes to its own dedicated backend DB server (whether `MySQL` or `sqlite`)

- The `ham4db` nodes communication is minimal. They do not share discovery info (since they each discover independently). Instead, the _leader_ shares with the other nodes what user instructions is intercepted, such as:

  - `begin-downtime`
  - `register-candidate`

    etc.

  The _leader_ will also educate its followers about ongoing failovers.

  The communication between `ham4db` node does not correlate to transactional database commits, and is sparse.

- All user changes must go through the leader, and in particular via the `HTTP API`. You must not manipulate the backend database directly, since such a change will not be published to the other nodes.

- As result, on a `ham4db/raft`, one may not use the `ham4db` executable in command line mode: an attempt to run `ham4db` cli will refuse to run when `raft` mode is enabled. Work is ongoing to allow some commands to run via cli.

- A utility script, [ham4db-client](client.md) is available that provides similar interface as the command line `ham4db`, and that uses & manipulates `HTTP API` calls.

- You will only install the `ham4db` binaries on `ham4db` service nodes, and no where else. The `ham4db-client` script can be installed wherever you wish to.

- A failure of a single `ham4db` node will not affect `ham4db`'s availability. On a `3` node setup at most one server may fail. On a `5` node setup `2` nodes may fail.

- An `ham4db` node cannot run without its backend DB. With `sqlite` backend this is trivial since `sqlite` runs embedded with `ham4db`. With `MySQL` backend, the `ham4db` service will bail out if unable to connect to the backend DB over a period of time.

- An `ham4db` node may be down, then come back. It will rejoin the `raft` group, and receive whatever events it missed while out. It does not matter how long the node has been away. If it does not have relevant local `raft` log/snapshots, another node will automatically feed it with a recent snapshot.

- The `ham4db` service will bail out if it can't join the `raft` group.

See also [Master discovery with Key Value stores](kv.md#kv-and-ham4db-raft) via `ham4db/raft`.

### Main advantages of ham4db/raft

- Highly available
- Consensus: failovers are made by leader node that is member of quorum (not isolated)
- Supports `SQLite` (embedded) backend, no need for `MySQL` backend though supported.
- Little cross-node communication ; fit for high latency cross DC networks

### DC fencing example

Consider this example of three data centers, `DC1`, `DC2` and `DC3`. We run `ham4db/raft` with three nodes, one in each data center.

![ham4db/raft, 3 DCs](images/ham4db-raft-3dc.png)

What happens when `DC2` gets network isolated?

![ham4db/raft, 3 DCs, DC2 view](images/ham4db-raft-3dc-dc2.png)

![ham4db/raft, 3 DCs, quorum view](images/ham4db-raft-3dc-quorum.png)

![ham4db/raft, 3 DCs, recovery](images/ham4db-raft-3dc-recovery.png)

### Roadmap

Still ongoing and TODO:

- Failure detection to require quorum agreement (i.e. a `DeadMaster` needs to be analyzed by multiple `ham4db` nodes) in order to kick failover/recovery.

- Support sharing of probing (mutually exclusive to the above): the `leader` will divide the list of servers to probe between all nodes. Potentially by data-center. This will reduce probing load (each MySQL server will be probed by a single node rather than all nodes). All `ham4db` nodes will see same picture as opposed to independent views.
