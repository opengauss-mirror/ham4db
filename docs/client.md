# ham4db-client

[ham4db-client](https://gitee.com/opengauss/ham4db/blob/master/resources/bin/ham4db-client) is a script that wraps API calls with convenient command line interface.

It can auto-determine the leader of an `ham4db` setup and in such case forward all requests to the leader.

It closely mimics the `ham4db` command line interface.

With `ham4db-client`, you:

- Do not need the `ham4db` binary installed everywhere; only on hosts where the service runs
- Do not need to deploy `ham4db` configuration everywhere; only on service hosts
- Do not need to make access to backend DB
- Need to have access to the HTTP api
- Need to set the `HAM4DB_API` environment variable.
  - Either provide a single endpoint for a proxy, e.g.
  ```shell
  export HAM4DB_API=https://ham4db.myservice.com:3000/api
  ```
  - Or provide all `ham4db` endpoints, and `ham4db-client` will auto-pick the leader (no need for proxy), e.g.
  ```shell
  export HAM4DB_API="https://ham4db.host1:3000/api https://ham4db.host2:3000/api https://ham4db.host3:3000/api"
  ```
- You may set up the environment in `/etc/profile.d/ham4db-client.sh`. If this file exists, it will be inlined by `ham4db-client`.

### Sample usage

Show currently known clusters (replication topologies):

    ham4db-client -c clusters

Discover, forget an instance:

    ham4db-client -c discover -i 127.0.0.1:22987
    ham4db-client -c forget -i 127.0.0.1:22987

Print an ASCII tree of topology instances. Pass a cluster name via `-i` (see `clusters` command above):

    ham4db-client -c topology -i 127.0.0.1:22987

> Sample output:
>
>     127.0.0.1:22987
>     + 127.0.0.1:22989
>       + 127.0.0.1:22988
>     + 127.0.0.1:22990

Move the replica around the topology:

    ham4db-client -c relocate -i 127.0.0.1:22988 -d 127.0.0.1:22987

> Resulting topology:
>
>     127.0.0.1:22987
>     + 127.0.0.1:22989
>     + 127.0.0.1:22988
>     + 127.0.0.1:22990

etc.

### Behind the scenes

The command line interface makes for a nice wrapper to API calls, whose output is then transformed from JSON format to textual format.

As example, the command:

```shell
ham4db-client -c discover -i 127.0.0.1:22987
```

Translates to (simplified here for convenience):

```shell
curl "$HAM4DB_API/discover/127.0.0.1/22987" | jq '.Detail | .Key'
```

### Meta commands

- `ham4db-client -c help`: list all available commands
- `ham4db-client -c which-api`: output the API endpoint `ham4db-client` would use to invoke a command. This is useful when multiple endpoints are provided via `$HAM4DB_API`.
- `ham4db-client -c api -path clusters`: invoke a generic HTTP API call (in this case `clusters`) and return the raw JSON response.
