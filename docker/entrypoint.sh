#!/bin/sh
if [ ! -e /etc/ham4db.conf.json ] ; then
cat <<EOF > /etc/ham4db.conf.json
{
  "Debug": true,
  "ListenAddress": ":3000",
  "MySQLTopologyUser": "${ORC_TOPOLOGY_USER:-ham4db}",
  "MySQLTopologyPassword": "${ORC_TOPOLOGY_PASSWORD:-ham4db}",
  "BackendDBHost": "${ORC_DB_HOST:-db}",
  "BackendDBPort": ${ORC_DB_PORT:-3306},
  "BackendDatabase": "${ORC_DB_NAME:-ham4db}",
  "BackendDBUser": "${ORC_USER:-orc_server_user}",
  "BackendDBPassword": "${ORC_PASSWORD:-orc_server_password}"
}
EOF
fi

exec /usr/local/ham4db/ham4db http
