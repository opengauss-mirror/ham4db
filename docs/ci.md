# CI tests

`ham4db` uses [GitHub Actions](https://help.github.com/en/actions) to run the following CI tests:

- Build (main): build, unit tests, integration tests, docs tests
- Upgrade: test a successful upgrade path
- System: run system tests backed by actual MySQL topology

## Build

Running on pull requests, the [main CI](https://gitee.com/opengauss/ham4db/blob/master/.github/workflows/main.yml) job validates the following:

- Validate source code is formatted using `gofmt`
- Build passes
- Unit tests pass
- Integration tests pass
  - Using `SQLite` backend
  - Using `MySQL` backend
- Documentation tests pass: ensuring pages and links are not orphaned.

The [action](https://gitee.com/opengauss/ham4db/actions?query=workflow%3ACI) completes by producing an artifact: an `ham4db` binary for Linux `amd64`. The artifact is kept for a couple months per GitHub Actions policy.

## Upgrade

[Upgrade](https://gitee.com/opengauss/ham4db/blob/master/.github/workflows/upgrade.yml) runs on pull requests, tests a successful upgrade path from previous version (ie `master`) to PR's branch. This mainly tests internal database structure changes. The test:

- Checks out `master` and run `ham4db`, once using `SQLite`, once using `MySQL`
- Checks out `HEAD` (PR's branch) and run `ham4db` using pre-existing `SQLite` and `MySQL` backends. Expect no error.

## System

[System tests](https://gitee.com/opengauss/ham4db/blob/master/.github/workflows/system.yml) run as a scheduled job. A system test:

- Sets up a [CI environment](https://gitee.com/opengauss/ham4db-ci-env) which includes:
  - A replication topology via [DBDeployer](https://www.dbdeployer.com/), with heartbeat injection
  - [HAProxy](http://www.haproxy.org/)
  - [Consul](https://www.consul.io/)
  - [consul-template](https://github.com/hashicorp/consul-template)
- Deploys `ham4db` as a service, `ham4db-client`
- Runs a series of tests where `ham4db` operates on the topology, e.g. refactors or fails over.
