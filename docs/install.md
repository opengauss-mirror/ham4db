# Installation

For production deployments, see [Ham4db deployment](deployment.md). The following text walks you through the manual way of installation and the necessary configuration to make it work.

The following assumes you will be using the same machine for both the `ham4db` binary and the MySQL backend.
If not, replace `127.0.0.1` with appropriate host name. Replace `orch_backend_password` with your own super secret password.

#### Extract ham4db binary and files

- Extract from tarball

  Extract the archive you've downloaded from https://gitee.com/opengauss/ham4db/releases
  For example, let's assume you wish to install `ham4db` under `/usr/local/ham4db`:

      sudo mkdir -p /usr/local
      sudo cd /usr/local
      sudo tar xzfv ham4db-1.0.tar.gz

- Install from `RPM`

  Installs onto `/usr/local/ham4db`. Execute:

      sudo rpm -i ham4db-1.0-1.x86_64.rpm


- Install from `DEB`

  Installs onto `/usr/local/ham4db`. Execute:

      sudo dpkg -i ham4db_1.0_amd64.deb

- Install from repository

  `ham4db` packages can be found in https://packagecloud.io/github/ham4db


#### Setup backend MySQL server

Setup a MySQL server for backend, and invoke the following:

    CREATE DATABASE IF NOT EXISTS ham4db;
    CREATE USER 'ham4db'@'127.0.0.1' IDENTIFIED BY 'orch_backend_password';
    GRANT ALL PRIVILEGES ON `ham4db`.* TO 'ham4db'@'127.0.0.1';

`Ham4db` uses a configuration file, located in either `/etc/ham4db.conf.json` or relative path to binary `conf/ham4db.conf.json` or
`ham4db.conf.json`.

Tip: the installed package includes a file called `ham4db.conf.json.sample` with some basic settings which you can use as baseline for `ham4db.conf.json`. It is found in `/usr/local/ham4db/ham4db-sample.conf.json` and you may also find `/usr/local/ham4db/ham4db-sample-sqlite.conf.json` which has a SQLite-oriented configuration. Those sample files are also available [on the `ham4db` repository](https://gitee.com/opengauss/ham4db/tree/master/conf).

Edit `ham4db.conf.json` to match the above as follows:

    ...
    "BackendDBHost": "127.0.0.1",
    "BackendDBPort": 3306,
    "BackendDatabase": "ham4db",
    "BackendDBUser": "ham4db",
    "BackendDBPassword": "orch_backend_password",
    ...

#### Grant access to ham4db on all your MySQL servers
For `ham4db` to detect your replication topologies, it must also have an account on each and every topology. At this stage this has to be the
same account (same user, same password) for all topologies. On each of your masters, issue the following:

    CREATE USER 'ham4db'@'orch_host' IDENTIFIED BY 'orch_topology_password';
    GRANT SUPER, PROCESS, REPLICATION SLAVE, RELOAD ON *.* TO 'ham4db'@'orch_host';
    GRANT SELECT ON mysql.slave_master_info TO 'ham4db'@'orch_host';
    GRANT SELECT ON ndbinfo.processes TO 'ham4db'@'orch_host'; -- Only for NDB Cluster

> `REPLICATION SLAVE` is required for `SHOW SLAVE HOSTS`, and for scanning binary logs in favor of [Pseudo GTID](#pseudo-gtid)
> `RELOAD` required for `RESET SLAVE` operation
> `PROCESS` required to see replica processes in `SHOW PROCESSLIST`
> On MySQL 5.6 and above, and if using `master_info_repository = 'TABLE'`, let ham4db have access
> to the `mysql.slave_master_info` table. This will allow ham4db to grab replication credentials if need be.

Replace `orch_host` with hostname or ham4db machine (or do your wildcards thing). Choose your password wisely. Edit `ham4db.conf.json` to match:

    "MySQLTopologyUser": "ham4db",
    "MySQLTopologyPassword": "orch_topology_password",


Consider moving `conf/ham4db.conf.json` to `/etc/ham4db.conf.json` (both locations are valid)

To execute `ham4db` in command line mode or in HTTP API only, all you need is the `ham4db` binary.
To enjoy the rich web interface, including topology visualizations and drag-and-drop topology changes, you will need
the `resources` directory and all that is underneath it. If you're unsure, don't touch; things are already in place.
