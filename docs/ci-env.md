# CI environment

An ancillary project, [ham4db-ci-env](https://gitee.com/opengauss/ham4db-ci-env), provides a MySQL replication environment with which one may evaluate/test `ham4db`. Use cases:

- You want to check `ham4db`'s behavior on a testing environment.
- You want to test failover and master discovery.
- You want to develop changes to `ham4db` and require a reproducible environment.

You may do all of the above if you already have some staging environment with a MySQL replication topology, or a [dbdeployer](https://www.dbdeployer.com/) setup, but `ham4db-ci-env` offers a Docker-based environment, reproducible and dependency-free.

`ham4db-ci-env` is the same environment used in [system CI](ci.md#system) and in [Dockerfile.system](docker.md#run-full-ci-environment).


# Setup

Clone `ham4db-ci-env` via SSH or HTTPS:
```shell
$ git clone git@github.com:openark/ham4db-ci-env.git
```
or
```shell
$ git clone https://gitee.com/opengauss/ham4db-ci-env.git
```

# Run environment

Requirement: Docker.

```shell
$ cd ham4db-ci-env
$ script/dock
```

This will build and run an environment which conists of:

- A replication topology via [DBDeployer](https://www.dbdeployer.com/), with heartbeat injection
- [HAProxy](http://www.haproxy.org/)
- [Consul](https://www.consul.io/)
- [consul-template](https://github.com/hashicorp/consul-template)

Docker will expose these ports:

- `10111`, `10112`, `10113`, `10114`: MySQL hosts in a replication topology. Initially, `10111` is the master.
- `13306`: exposed by HAProxy and routed to current MySQL topology master.

# Run ham4db with environment

Assuming `ham4db` is built into `bin/ham4db` (`./script/build` if not):
```shell
$ bin/ham4db --config=conf/ci-env.conf.json --debug http
```

[`conf/ham4db-ci-env.conf.json`](https://gitee.com/opengauss/ham4db/blob/master/conf/ham4db-ci-env.conf.json) is designed to work with `ham4db-ci-env`.

You may choose to change the value of `SQLite3DataFile`, which is by default on `/tmp`.

# Running system tests with environment

While `ham4db` is running (see above), open another terminal in `ham4db`'s repo path.

Run:
```shell
$ ./tests/system/test.sh
```
for all tests, or
```shell
$ ./tests/system/test.sh <name-or-regex>
```
for a specific test, e.g. `./tests/system/test.sh relocate-single`

Destructive tests (e.g. a failover) require a full rebuild of the replication topology. The system tests CI runs both ham4db and the ci-env together, and the tests can instruct the ci-env to rebuild replication. However, if you run ci-env on a local docker, your tests cannot instruct a replication rebuild. You will need to manually run `./script/deploy-replication` on your ci-env container at the end of a destructive test.
