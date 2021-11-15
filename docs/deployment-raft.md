# Ham4db deployment: raft

This text describes deployments for [ham4db on raft](raft.md).

This complements general [deployment](deployment.md) documentation.

### Backend DB

You may choose between using `MySQL` and `SQLite`. See [backend configuration](configuration-backend.md).

- For MySQL:
  - The backend servers will be standalone. No replication setup. Each `ham4db` node will interact with its own dedicated backend server.
  - You _must_ have a `1:1` mapping `ham4db:MySQL`.
  - Suggestion: run `ham4db` and its dedicated MySQL server on same box.
  - Make sure to `GRANT` privileges for the `ham4db` user on each backend node.

- For `SQLite`:
  - `SQLite` is bundled with `ham4db`.
  - Make sure the `SQLite3DataFile` is writable by the `ham4db` user.

### High availability

`ham4db` high availability is gained by using `raft`. You do not need to take care of backend DB high availability.

### What to deploy: service

- Deploy the `ham4db` service onto service boxes.
  As suggested, you may want to put `ham4db` service and `MySQL` service on same box. If using `SQLite` there's nothing else to do.

- Consider adding a proxy on top of the service boxes; the proxy would redirect all traffic to the leader node. There is one and only one leader node, and the status check endpoint is `/api/leader-check`.
  - Clients may only interact with healthy raft nodes.
    - Simplest is to just interact with the leader. Setting up a proxy is one way to ensure that. See [proxy: leader section](raft.md#proxy-leader).
    -  Otherwise all healthy raft nodes will reverse proxy your requests to the leader. See [proxy: healthy raft nodes section](raft.md#proxy-healthy-raft-nodes).
- Nothing should directly interact with a backend DB. Only the leader is capable of coordinating changes to the data with the other `raft` nodes.

- `ham4db` nodes communicate between themselves on `DefaultRaftPort`. This port should be open to all `ham4db` nodes, and no one else needs to have access to this port.

### What to deploy: client

To interact with ham4db from shell/automation/scripts, you may choose to:

- Directly interact with the HTTP API
  - You may only interact with the _leader_. A good way to achieve this is using a proxy.
- Use the [ham4db-client](client.md) script.
  - Deploy `ham4db-client` on any box from which you wish to interact with `ham4db`.
  - Create and edit `/etc/profile.d/ham4db-client.sh` on those boxes to read:
    ```
    HAM4DB_API="http://your.ham4db.service.proxy:80/api"
    ```
    or
    ```
    HAM4DB_API="http://your.ham4db.service.host1:3000/api http://your.ham4db.service.host2:3000/api http://your.ham4db.service.host3:3000/api"
    ```
    In the latter case you will provide the list of all `ham4db` nodes, and the `ham4db-client` script will automatically figure out which is the leader. With this setup your automation will not need a proxy (though you may still wish to use a proxy for web interface users).

    Make sure to chef/puppet/whatever the `HAM4DB_API` value such that it adapts to changes in your environment.

- The `ham4db` command line client will refuse to run given a raft setup, since it interacts directly with the underlying database and doesn't participate in the raft consensus, and thus cannot ensure all raft members will get visibility into it changes.
  - Fortunately `ham4db-client` provides an almost identical interface as the command line client.
  - You may force the command line client to run via `--ignore-raft-setup`. This is a "I know what I'm doing" risk you take. If you do choose to use it, then it makes more sense to connect to the leader's backend DB.


### Ham4db service

As noted, a single `ham4db` node will assume leadership. Only the leader will:

- Run recoveries

However all nodes will:

- Discover (probe) your MySQL topologies
- Run failure detection
- Register their own health check

Non-leader nodes must _NOT_:

- Run arbitrary commands (e.g. `relocate`, `begin-downtime`)
- Run recoveries per human request.
- Serve client HTTP requests (but some endpoints, such as load-balancer and health checks, are valid).

### A visual example

![ham4db deployment, raft](images/ham4db-deployment-raft.png)

In the above there are three `ham4db` nodes in a `raft` cluster, each using its own dedicated database (either `MySQL` or `SQLite`).

`ham4db` nodes communicate with each other.

Only one `ham4db` node is the leader.

All `ham4db` nodes probe the entire `MySQL` fleet. Each `MySQL` server is probed by each of the `raft` members.

### ham4db/raft operational scenarios

##### A node crashes:

Start the node, start the `MySQL` service if applicable, start the `ham4db` service. The `ham4db` service should join the `raft` group, get a recent snapshot, catch up with `raft` replication log and continue as normal.

##### A new node is provisioned / a node is re-provisioned

Such that the backend database is completely empty/missing.

- If `SQLite`, nothing to be done. The node will just join the `raft` group, get a snapshot from one of the active nodes, catch up with `raft` log and run as normal.
- If `MySQL`, the same will be attempted. However, the `MySQL` server will have to [have the privileges](configuration-backend.md#mysql-backend-db-setup) for `ham4db` to operate. So if this is a brand new server, those privileges are likely to not be there.
  As example, our `puppet` setup periodically ensures privileges are set on our MySQL servers. Thus, when a new server is provisioned, next `puppet` run lays the privileges for `ham4db`. `puppet` also ensures the `ham4db` service is running, and so, pending some time, `ham4db` can automatically join the group.

##### Cloning is valid

If you choose to, you may also provision new boxes by cloning your existing backend databases using your favorite backup/restore  or dump/load method.

This is **perfectly valid** though **not required**.

- If `MySQL`, run backup/restore, either logical or physical.
- If `SQLite`, run `.dump` + restore, see [10. Converting An Entire Database To An ASCII Text File](https://sqlite.org/cli.html).

- Start the `ham4db` service. It should catch up with `raft` replication log and join the `raft` cluster.

##### Replacing a node

Assuming `RaftNodes: ["node1", "node2", "node3"]`, and you wish to replace `node3` with `nodeX`.

- You may take down `node3`, and the `raft` cluster will continue to work as long as `node1` and `node2` are good.
- Create `nodeX` box. Generate backend db data (see above).
- On `node1`, `node2` and `nodeX` reconfigure `RaftNodes: ["node1", "node2", "nodeX"]`.
- Start `ham4db` on `nodeX`. It will be refused and will not join the cluster because `node1` and `node2` are not yet aware of the change.
- Restart `ham4db` on `node1`.
- Restart `ham4db` on `node2`.
  - All three nodes should form a happy cluster at this time.
