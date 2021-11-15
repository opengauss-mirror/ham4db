# About HAM4DB

`ham4db` is a MySQL replication topology HA, management and visualization tool, allowing for:

#### Discovery

`ham4db` actively crawls through your topologies and maps them. It reads basic MySQL info such as replication status and configuration.

It provides with slick visualization of your topologies, including replication problems, even in the face of failures.

#### Refactoring

`ham4db` understands replication rules. It knows about binlog file:position, GTID, Pseudo GTID, Binlog Servers.

Refactoring replication topologies can be a matter of drag & drop a replica under another master. Moving replicas around becomes
safe: `ham4db` will reject an illegal refactoring attempt.

Find grained control is achieved by various command line options.

#### Recovery

`ham4db` uses a holistic approach to detect master and intermediate master failures. Based on information gained from the topology itself, it recognizes a variety of failure scenarios.

Configurable, it may choose to perform automated recovery (or allow the user to choose type of manual recovery). Intermediate master recovery achieved internally to `ham4db`. Master failover supported by pre/post failure hooks.

Recovery process utilizes _ham4db's_ understanding of the topology and of its ability to perform refactoring. It is based on _state_ as opposed to _configuration_: `ham4db` picks the best recovery method by investigating/evaluating the topology at the time of
recovery itself.

![ham4db screenshot](images/ham4db-simple.png)

#### Credits, attributions

Authored by [SANGFOR TECHNOLOGIES](https://github.com/shlomi-noach)

This project was originally initiated at [Outbrain](http://outbrain.com), who were kind enough to release it as open source from its very beginning. We wish to recognize Outbrain for their support of open source software and for their further willingness to collaborate to this particular project's success.

The project was later developed at [Booking.com](http://booking.com) and the company was gracious enough to release further changes into the open source.

At this time the project is being developed at [GitHub](http://github.com).
We will continue to keep it open and supported.

The project accepts pull-requests and encourages them. Thank you for any improvement/assistance!

Additional collaborators & contributors to this Wiki:

- [grierj](https://github.com/grierj)
- Other awesome people
