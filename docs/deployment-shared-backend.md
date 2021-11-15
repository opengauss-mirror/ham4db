# Ham4db deployment: shared backend

This text describes deployments for shared backend DB. See [High availability](high-availability.md) for the various backend DB setups.

This complements general [deployment](deployment.md) documentation.

### Shared backend

You will need to set up a shared backend database. This could be synchronous replication (Galera/XtraDB Cluster/InnoDB Cluster) for high availability, or it could be a master-replicas setup etc.

The backend database has the _state_ of your topologies. `ham4db` itself is almost stateless, and trusts the data in the backend database.

In a shared backend setup multiple `ham4db` services will all speak to the same backend.

- For **synchronous replication**, the advice is:
  - Configure multi-writer mode (each node in the MySQL cluster is writable)
  - Have `1:1` mapping between `ham4db` services and `MySQL` nodes: each `ham4db` service to speak with its own node.
- For **master-replicas** (asynchronous & semi-synchronous), do:
  - Configure all `ham4db` nodes to access the _same_ backend DB (the master)
  - Optionally you will have your own load balancer to direct traffic to said master, in which case configure all `ham4db` nodes to access the proxy.

### MySQL backend setup and high availability

Setting up the backend DB is on you. Also, `orchestartor` doesn't eat its own dog food, and cannot recover a failure on its own backend DB.
You will need to handle, for example, the issue of adding a Galera node, or of managing your proxy health checks etc.

### What to deploy: service

- Deploy the `ham4db` service onto service boxes. The decision of how many service boxes  to deploy
  will depend on your [availability needs](high-availability.md).
  - In a synchronous replication shared backend setup, these may well be the very MySQL boxes, in a `1:1` mapping.
- Consider adding a proxy on top of the service boxes; the proxy would ideally redirect all traffic to the leader node. There is one and only one leader node, and the status check endpoint is `/api/leader-check`. It is OK to direct traffic to any healthy service. Since all `ham4db` nodes speak to the same shared backend DB, it is OK to operate some actions from one service node, and other actions from another service nodes. Internal locks are placed to avoid running contradicting or interfering commands.


### What to deploy: client

To interact with ham4db from shell/automation/scripts, you may choose to:

- Directly interact with the HTTP API
- use the [ham4db-client](client.md) script.
  - Deploy `ham4db-client` on any box from which you wish to interact with `ham4db`.
  - Create and edit `/etc/profile.d/ham4db-client.sh` on those boxes to read:
    ```
    HAM4DB_API="http://your.ham4db.service.proxy:80/api"
    ```
    or
    ```
    HAM4DB_API="http://your.ham4db.service.host1:3000/api http://your.ham4db.service.host2:3000/api http://your.ham4db.service.host3:3000/api"
    ```
    In the latter case you will provide the list of all `ham4db` nodes, and the `orchetsrator-client` script will automatically figure out which is the leader. With this setup your automation will not need a proxy (though you may still wish to use a proxy for web interface users).

    Make sure to chef/puppet/whatever the `HAM4DB_API` value such that it adapts to changes in your environment.

- The [ham4db command line](executing-via-command-line.md).
  - Deploy the `ham4db` binary (you may use the `ham4db-cli` distributed package) on any box from which you wish to interact with `ham4db`.
  - Create `/etc/ham4db.conf.json` on those boxes, populate with credentials. This file should generally be the same as for the `ham4db` service boxes. If you're unsure, use exact same file content.
  - The `ham4db` binary will access the shared backend DB. Make sure to give it access. Typicaly this will be port `3306`.

It is OK to run `ham4db` CLI even while the `ham4db` service is operating, since they will all coordinate on the same backend DB.

### Ham4db service

In a shared-backend deployment, you may deploy the number of `ham4db` nodes as suits your requirements.

However, as noted, one `ham4db` node will be [elected leader](http://code.openark.org/blog/mysql/leader-election-using-mysql). Only the leader will:

- Discover (probe) your MySQL topologies
- Run failure detection
- Run recoveries

All nodes will:

- Serve HTTP requests
- Register their own health check

All nodes may:

- Run arbitrary command (e.g. `relocate`, `begin-downtime`)
- Run recoveries per human request.

For more details about deploying multiple nodes, please read about [high availability](high-availability.md).

### Ham4db CLI

The CLI executes to fulfill a specific operation. It may choose to probe a few servers, depending on the operation (e.g. `relocate`), or it may probe no server at all and just read data from the backend DB.

### A visual example

![ham4db deployment, shared backend](images/ham4db-deployment-shared-backend.png)

In the above there are three `ham4db` nodes running on top of a `3` node synchronous replication setup. Each `ham4db` nodes speaks to a different `MySQL` backend, but those are replicated synchronously and all share the same picture (up to some lag).

One `ham4db` node is elected as leader, and only that node probes the MySQL topologies. It probes all known servers (the above image only shows part of the probes to avoid the spaghetti).
