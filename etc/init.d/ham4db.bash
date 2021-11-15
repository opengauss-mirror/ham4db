#!/bin/bash
# ham4db daemon
# chkconfig: 345 20 80
# description: ham4db daemon
# processname: ham4db
#
### BEGIN INIT INFO
# Provides:          ham4db
# Required-Start:    $local_fs $syslog
# Required-Stop:     $local_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start ham4db daemon
# Description:       Start ham4db daemon
### END INIT INFO


# Script credit: http://werxltd.com/wp/2012/01/05/simple-init-d-script-template/

DAEMON_PATH="/usr/local/ham4db"

DAEMON=ham4db
DAEMONOPTS="--verbose http"

NAME=ham4db
DESC="ham4db:  ha manager for database"
PIDFILE=/var/run/$NAME.pid
SCRIPTNAME=/etc/init.d/$NAME

# Limit the number of file descriptors (and sockets) used by
# ham4db.  This setting should be fine in most cases but a
# large busy environment may # reach this limit. If exceeded expect
# to see errors of the form:
#   2017-06-12 02:33:09 ERROR dial tcp 10.1.2.3:3306: connect: cannot assign requested address
# To avoid touching this script you can use /etc/ham4db_profile
# to increase this limit.
ulimit -n 16384

# initially noop but can adjust according by modifying ham4db_profile
# - see https://gitee.com/opengauss/orchestrator/issues/227 for more details.
post_start_daemon_hook () {
	# by default do nothing
	:
}

# Start the ham4db daemon in the background
start_daemon () {
	# start up daemon in the background
	$DAEMON_PATH/$DAEMON $DAEMONOPTS >> /var/log/${NAME}.log 2>&1 &
	# collect and print PID of started process
	echo $!
	# space for optional processing after starting ham4db
	# - redirect stdout to stderro to prevent this corrupting the pid info
	post_start_daemon_hook 1>&2
}

# This files can be used to inject pre-service execution
# scripts, such as exporting variables or whatever. It's yours!
[ -f /etc/default/ham4db ] && . /etc/default/ham4db
[ -f /etc/ham4db_profile ] && . /etc/ham4db_profile
[ -f /etc/profile.d/ham4db ] && . /etc/profile.d/ham4db

case "$1" in
  start)
    printf "%-50s" "Starting $NAME..."
    cd $DAEMON_PATH
    PID=$(start_daemon)
    #echo "Saving PID" $PID " to " $PIDFILE
    if [ -z $PID ]; then
      printf "%s\n" "Fail"
      exit 1
    elif [ -z "$(ps axf | awk '{print $1}' | grep ${PID})" ]; then
      printf "%s\n" "Fail"
      exit 1
    else
      echo $PID > $PIDFILE
      printf "%s\n" "Ok"
    fi
  ;;
  status)
    printf "%-50s" "Checking $NAME..."
    if [ -f $PIDFILE ]; then
      PID=$(cat $PIDFILE)
      if [ -z "$(ps axf | awk '{print $1}' | grep ${PID})" ]; then
        printf "%s\n" "Process dead but pidfile exists"
        exit 1
      else
        echo "Running"
      fi
    else
      printf "%s\n" "Service not running"
      exit 1
    fi
  ;;
  stop)
    printf "%-50s" "Stopping $NAME"
    PID=$(cat $PIDFILE)
    cd $DAEMON_PATH
    if [ -f $PIDFILE ]; then
      kill -TERM $PID
      rm -f $PIDFILE
      # Wait for ham4db to stop otherwise restart may fail.
      # (The newly restarted process may be unable to bind to the
      # currently bound socket.)
      while ps -p $PID >/dev/null 2>&1; do
        printf "."
        sleep 1
      done
      printf "\n"
      printf "Ok\n"
    else
      printf "%s\n" "pidfile not found"
      exit 1
    fi
  ;;
  restart)
    $0 stop
    $0 start
  ;;
  reload)
    PID=$(cat $PIDFILE)
    cd $DAEMON_PATH
    if [ -f $PIDFILE ]; then
      kill -HUP $PID
      printf "%s\n" "Ok"
    else
      printf "%s\n" "pidfile not found"
      exit 1
    fi
	;;
  *)
    echo "Usage: $0 {status|start|stop|restart|reload}"
    exit 1
esac
