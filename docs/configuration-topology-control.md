# Configuration: topology control

The following configuration affects how `ham4db` applies changes to topology servers:

`ham4db` will figure out the name of the cluster, data center, and more.

```json
{
  "UseSuperReadOnly": false,
}
```

### UseSuperReadOnly

By default `false`. When `true`, whenever `ham4db` is asked to set/clear `read_only`, it will also apply the change to `super_read_only`. `super_read_only` is only available on Oracle MySQL and Percona Server, as of specific versions.
