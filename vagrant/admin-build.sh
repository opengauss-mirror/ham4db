#!/bin/bash
set -xeuo pipefail
# Install ham4db
if [[ -e /etc/redhat-release ]]; then
  rpm -i /tmp/ham4db-release/ham4db*.rpm
fi

if [[ -e /etc/debian_version ]]; then
  dpkg -i /tmp/ham4db-release/ham4db*.deb
fi

if [[ -e /ham4db/vagrant/.sqlite ]]; then
  cp -fv /usr/local/ham4db/sample-sqlite.conf.json /etc/ham4db.conf.json
else
  cp -fv /usr/local/ham4db/sample.conf.json /etc/ham4db.conf.json
fi

if [[ -e /etc/redhat-release ]]; then

  /sbin/chkconfig ham4db on
  /sbin/service ham4db start

elif [[ -e /etc/debian_version ]]; then

  update-rc.d ham4db defaults
  /usr/sbin/service ham4db start

fi

echo '* * * * * root /usr/bin/ham4db -c discover -i db1' > /etc/cron.d/ham4db-discovery

# Discover instances
/usr/bin/ham4db --verbose --debug --stack -c discover -i localhost
