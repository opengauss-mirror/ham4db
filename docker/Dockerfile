# Requires Docker 17.09 or later (multi stage builds)
#
# Ham4db will look for a configuration file at /etc/ham4db.conf.json.
# It will listen on port 3000.
# If not present a minimal configuration will be generated using the following environment variables:
#
# Default variables which can be used are:
#
# ORC_TOPOLOGY_USER (default: ham4db): username used by ham4db to login to MySQL when polling/discovering
# ORC_TOPOLOGY_PASSWORD (default: ham4db):  password needed to login to MySQL when polling/discovering
# ORC_DB_HOST (default: ham4db):  ham4db backend MySQL host
# ORC_DB_PORT (default: 3306):  port used by ham4db backend MySQL server
# ORC_DB_NAME (default: ham4db): database named used by ham4db backend MySQL server
# ORC_USER (default: orc_server_user): username used to login to ham4db backend MySQL server
# ORC_PASSWORD (default: orc_server_password): password used to login to ham4db backend MySQL server

FROM golang:1.14.4-alpine3.12 as build

ENV GOPATH=/tmp/go

RUN apk --no-cache add libcurl rsync gcc g++ build-base bash git

RUN mkdir -p $GOPATH/src/gitee.com/opengauss/ham4db
WORKDIR $GOPATH/src/gitee.com/opengauss/ham4db
COPY . .
RUN bash build.sh -b -P
RUN rsync -av $(find /tmp/ham4db-release -type d -name ham4db -maxdepth 2)/ /
RUN rsync -av $(find /tmp/ham4db-release -type d -name ham4db-client -maxdepth 2)/ /
RUN cp conf/ham4db-sample-sqlite.conf.json /etc/ham4db.conf.json

FROM alpine:3.8

RUN apk --no-cache add bash curl jq

EXPOSE 3000

COPY --from=build /usr/local/ham4db /usr/local/ham4db
COPY --from=build /usr/bin/ham4db-client /usr/bin/ham4db-client
COPY --from=build /etc/ham4db.conf.json /etc/ham4db.conf.json

WORKDIR /usr/local/ham4db
ADD docker/entrypoint.sh /entrypoint.sh
CMD /entrypoint.sh
