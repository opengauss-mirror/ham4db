# Configuration: backend

Let ham4db know where to find backend database. In this setup `ham4db` will serve HTTP on port `:3000`.

```json
{
  "Debug": false,
  "ListenAddress": ":3000",
}
```

You may choose either a `MySQL` backend or a `SQLite` backend. See [High availability](high-availability.md) page for scenarios, possibilities and reasons to using either.

## MySQL backend

You will need to set up schema & credentials:

```json
{
  "BackendDBHost": "ham4db.backend.master.com",
  "BackendDBPort": 3306,
  "BackendDatabase": "ham4db",
  "MySQLCredentialsConfigFile": "/etc/mysql/ham4db-backend.cnf"
}
```

Notice `MySQLCredentialsConfigFile`. It will be of the form:
```
[client]
user=ham4db_srv
password=${HAM4DB_PASSWORD}
```

where either `user` or `password` can be in plaintext or get their value from the environment.


Alternatively, you may choose to use plaintext credentials in the config file:

```json
{
  "BackendDBUser": "ham4db_srv",
  "BackendDBPassword": "orc_server_password",
}
```

#### MySQL backend DB setup

For a MySQL backend DB, you will need to grant the necessary privileges:

```
CREATE USER 'ham4db_srv'@'orc_host' IDENTIFIED BY 'orc_server_password';
GRANT ALL ON ham4db.* TO 'ham4db_srv'@'orc_host';
```

## SQLite backend

Default backend is `MySQL`. To setup `SQLite`, use:

```json
{
  "BackendDB": "sqlite3",
  "SQLite3DataFile": "/var/lib/ham4db/ham4db.db",  
}
```

`SQLite` is embedded within `ham4db`.

If the file indicated by `SQLite3DataFile` does not exist, `ham4db` will create it. It will need write permissions on given path/file.
