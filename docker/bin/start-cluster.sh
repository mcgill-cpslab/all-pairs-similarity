#!/bin/bash

BASE_DIR=`pwd`

WORKER_NUM=$1

#!/bin/bash
# Credits to the author of docker-scripts
# https://github.com/amplab/docker-scripts/blob/47230392fdde9af67ed9d63927c00cfb9ac13b6d/deploy/start_nameserver.sh

set -x

NAMESERVER=-1
NAMESERVER_IP=
DOMAINNAME=
#".mycluster.com"

function reverse_ip() {
  IP=$1
  OLD_IFS=$IFS
  IFS='.'
  IP_SPLIT=($IP)
  REVERSED_IP=${IP_SPLIT[0]}
  for (( idx=1 ; idx<${#IP_SPLIT[@]} ; idx++ )) ; do
    REVERSED_IP="${IP_SPLIT[idx]}.$REVERSED_IP"
  done
  IFS=$OLD_IFS
}

# starts the dnsmasq nameserver
function start_nameserver() {
    DNSDIR="/tmp/dnsdir_$RANDOM"
    DNSFILE="${DNSDIR}/0hosts"
    mkdir $DNSDIR

    echo "starting nameserver container"
    NAMESERVER=$(docker run -d -h nameserver${DOMAINNAME} -v $DNSDIR:/etc/dnsmasq.d $1)

    if [ "$NAMESERVER" = "" ]; then
        echo "error: could not start nameserver container from image $1"
        exit 1
    fi

    echo "started nameserver container:  $NAMESERVER"
    echo "DNS host->IP file mapped:      $DNSFILE"
    sleep 2
    NAMESERVER_IP=$(docker logs $NAMESERVER 2>&1 | egrep '^NAMESERVER_IP=' | awk -F= '{print $2}' | tr -d -c "[:digit:] .")
    echo "NAMESERVER_IP:                 $NAMESERVER_IP"
    echo "address=\"/nameserver/$NAMESERVER_IP\"" > $DNSFILE
    # add reverse dns record
    reverse_ip $NAMESERVER_IP
    echo "ptr-record=$REVERSED_IP.in-addr.arpa,nameserver" >> $DNSFILE
}

# contact nameserver container and resolve IP address (used for checking whether nameserver has registered
# presence of new container). note: only returns exit code
function check_hostname() {
    local __resultvar=$1
    local val_hostname=$2
    local val_expected_ip=$3
    DNSCMD="nslookup $val_hostname $NAMESERVER_IP | grep Address | tail -n 1 | grep $val_expected_ip > /dev/null"
    #echo "DNSCMD: $DNSCMD"
    eval $DNSCMD
    eval $__resultvar=$?
}

# contact nameserver container and resolve IP address
function resolve_hostname() {
    local __resultvar=$1
    local val_hostname=$2
    DNSCMD="nslookup $val_hostname $NAMESERVER_IP | grep Address | tail -n 1 | awk -F":" '{print \$2}' | awk '{print \$1}'"
    #echo "DNSCMD: $DNSCMD"
    tmpval=$(eval "$DNSCMD")
    eval $__resultvar="$tmpval"
}

function wait_for_nameserver {
    echo -n "waiting for nameserver to come up "
    # Note: the original scripts assumed the nameserver resolves its own
    # hostname to 127.0.0.1
    # With newer versions of Docker that is not necessarily the case anymore.
    # Thanks to bmustafa (24601 on GitHub) for reporting and proposing a fix!
    check_hostname result nameserver "$NAMESERVER_IP"
    until [ "$result" -eq 0 ]; do
        echo -n "."
        sleep 1
        check_hostname result nameserver "$NAMESERVER_IP"
    done
    echo ""
}

function start_cluster() {
  echo "starting all-pairs cluster"
  for i in `seq 1 $WORKER_NUM`; do
    hostname="worker${i}"
    docker run -d -h $hostname --dns $2 -v $BASE_DIR:/root/app -v /Users/nanzhu/code/all-pairs-similarity/data/maildir_small:/root/data $1
  done

  sleep 3
}

start_nameserver codingcat/dev:nameserver
wait_for_nameserver
start_cluster codingcat/all-pairs $2
