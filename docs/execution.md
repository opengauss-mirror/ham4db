# Execution

#### Executing as web/API service

Assuming you've installed `ham4db` under `/usr/local/ham4db`:

    cd /usr/local/ham4db && ./ham4db http

`Ham4DB` will start listening on port `3000`. Point your browser to `http://your.host:3000/`
and you're ready to go. You may skip to next sections.

If you like your debug messages, issue:

    cd /usr/local/ham4db && ./ham4db --debug http

or, even more detailed in case of error:

    cd /usr/local/ham4db && ./ham4db --debug --stack http

The above looks for configuration in `/etc/ham4db.conf.json`, `conf/ham4db.conf.json`, `ham4db.conf.json`, in that order.
Classic is to put configuration in `/etc/ham4db.conf.json`. Since it contains credentials to your MySQL servers you may wish to limit access to that file.
You may choose to use a different location for the configuration file, in which case execute:

    cd /usr/local/ham4db && ./ham4db --debug --config=/path/to/config.file http

Web/API service will, by default, issue a continuous, infinite polling of all known servers. This keeps `ham4db`'s data up to date.
You typically want this behavior, but you may disable it, making `ham4db` just serve API/Web but never update the instances status:

    cd /usr/local/ham4db && ./ham4db --discovery=false http

The above is useful for development and testing purposes. You probably wish to keep to the defaults.
