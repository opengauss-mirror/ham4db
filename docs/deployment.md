# Ham4db deployment in production

What does an `ham4db` deployment look like? What do you need to set in `puppet/chef`? What services need to run and where?

## Deploying the service & clients

You will first decide whether you want to run `ham4db` on a shared backend DB or with a `raft` setup. See [High availability](high-availability.md) for some options, and [ham4db/raft vs. synchronous replication setup](raft-vs-sync-repl.md) for comparison & discussion.

Follow these deployment guides:

- Deploying `ham4db` on [shared backend DB](deployment-shared-backend.md)
- Deploying `ham4db` via [raft consensus](deployment-raft.md)

## Next steps

`ham4db` works well in dynamic environments and adapts to changes in inventory, configuration and topology. Its dynamic nature suggests that the environment should interact with it in dynamic nature, as well. Instead of hard-coded configuration, `ham4db` is happy to accept dynamic hints and requests that change its perspective on the topologies. Once the `ham4db` services and clients are deployed, consider performing the following actions to take full
advantage of this.

### Discovering topologies

`ham4db` auto-discovers boxes joining a topology. If a new replica joins an existing cluster that is monitored by `ham4db`, it is discovered when `ham4db` next probes its master.

However, how does `ham4db` discover completely new topologies?

- You may ask `ham4db` to _discover_ (probe) any single server in such a topology, and from there on it will crawl its way across the entire topology.
- Or you may choose to just let `ham4db` know about any single production server you have, routinely. Set up a cronjob on any production `MySQL` server to read:

  ```
  0 0 * * * root "/usr/bin/perl -le 'sleep rand 600' && /usr/bin/ham4db-client -c discover -i this.hostname.com"
  ```

  In the above, each host lets `ham4db` know about itself once per day; newly bootstrapped hosts are discovered the next midnight. The `sleep` in introduced to avoid storming `ham4db` by all servers at the same time.

  The above uses [ham4db-client](client.md), but you may use the [ham4db cli](executing-via-command-line.md) if running on a shared-backend setup.

### Adding promotion rules

Some servers are better candidate for promotion in the event of failovers. Some servers aren't good picks. Examples:

- A server has weaker hardware configuration. You prefer to not promote it.
- A server is in a remote data center and you don't want to promote it.
- A server is used as your backup source and has LVM snapshots open at all times. You don't want to promote it.
- A server has a good setup and is ideal as candidate. You prefer to promote it.
- A server is OK, and you don't have any particular opinion.

You will announce your preference for a given server to `ham4db` in the following way:

```
ham4db -c register-candidate -i ${::fqdn} --promotion-rule ${promotion_rule}
```

Supported promotion rules are:

- `prefer`
- `neutral`
- `prefer_not`
- `must_not`

Promotion rules expire after an hour. That's the dynamic nature of `ham4db`. You will want to setup a cron job that will announce the promotion rule for a server:

```
*/2 * * * * root "/usr/bin/perl -le 'sleep rand 10' && /usr/bin/ham4db-client -c register-candidate -i this.hostname.com --promotion-rule prefer"
```

This setup comes from production environments. The cron entries get updated by `puppet` to reflect the appropriate `promotion_rule`. A server may have `prefer` at this time, and `prefer_not` in 5 minutes from now. Integrate your own service discovery method, your own scripting, to provide with your up-to-date `promotion-rule`.

### Downtiming

When a server has a problem, it:

- Will show up in the `problems` dropdown on the web interface.
- May be considered for recovery (example: server is dead and all of it's replicas are now broken).

You may _downtime_ a server such that:
- It will not show up in the `problems` dropdown.
- It will not be considered for recovery.

Downtiming takes place via:

```
ham4db-client -c begin-downtime -duration 30m -reason "testing" -owner myself
```

Some servers may be known to be routinely broken; for example, auto-restore servers; dev boxes; testing boxes. For such servers you may want to have _continuous_ downtime. One way to achieve that it to set so large `-duration 240000h`. But then you need to remember to `end-downtime` if something changes about the box. Continuing the dynamic approach, consider:

```
*/2 * * * * root "/usr/bin/perl -le 'sleep rand 10' && /data/ham4db/current/bin/ham4db -c begin-downtime -i ${::fqdn} --duration=5m --owner=cron --reason=continuous_downtime"
```

Every `2` minutes, downtime for `5` minutes; this means that as we cancel the cronjob, _downtime_ will expire within `5` minutes.

Shown above are uses for both `ham4db-client` and the `ham4db` command line interface. For completeness, this is how to operate the same via direct API call:

```shell
$ curl -s "http://my.ham4db.service:80/api/begin-downtime/my.hostname/3306/wallace/experimenting+failover/45m"
```

The `ham4db-client` script runs this very API call, wrapping it up and encoding the URL path. It can also automatically detect the leader, in case you don't want to run through a proxy.

### Pseudo-GTID

If you're not using GTID, you'll be happy to know `ham4db` can utilize Pseudo-GTID to achieve similar benefits to GTID, such as correlating two unrelated servers and making one replicate from the other. This implies master and intermediate master failovers.

Read more on the [Pseudo-GTID](pseudo-gtid.md) documentation page.

`ham4db` can inject Pseudo-GTID entries for you. Your clusters will magically have GTID-like superpowers. Follow [Automated Pseudo-GTID](configuration-discovery-pseudo-gtid.md#automated-pseudo-gtid-injection)

### Populating meta data

`ham4db` extracts some metadata from servers:
- What's the alias for the cluster this instance belongs to?
- What's the data center a server belongs to?
- Is semi-sync enforced on this server?

These details are extracted by queries such as:
- `DetectClusterAliasQuery`
- `DetectClusterDomainQuery`
- `DetectDataCenterQuery`
- `DetectSemiSyncEnforcedQuery`

or by regular expressions acting on the hostnames:
- `DataCenterPattern`
- `EnvironmentPattern`

Queries can be satisfied by injecting data into metadata tables on your master. For example, you may:

```sql
CREATE TABLE IF NOT EXISTS cluster (
  anchor TINYINT NOT NULL,
  cluster_name VARCHAR(128) CHARSET ascii NOT NULL DEFAULT '',
  PRIMARY KEY (anchor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```

and populate this table, with, say `1, my_cluster_name`, coupled with:
```json
{
  "DetectClusterAliasQuery": "select cluster_name from meta.cluster where anchor=1"
}
```

Please note `ham4db` does not create such tables nor does it populate them.
You will need to create the table, populate them, and let `ham4db` know how to query the data.

### Tagging

`ham4db` supports tagging of instances, as well as searching for instances by tags. See [Tags](tags.md)
