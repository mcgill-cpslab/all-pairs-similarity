#!/bin/bash

BASE_DIR=`pwd`

WORKER_NUM=$1

#!/bin/bash
# Credits to the author of docker-scripts
# https://github.com/amplab/docker-scripts/blob/47230392fdde9af67ed9d63927c00cfb9ac13b6d/deploy/start_nameserver.sh

set -x

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

function start_cluster() {
  echo "starting all-pairs cluster"
  for i in `seq 1 $WORKER_NUM`; do
    hostname="compute-node-${i}"
    docker run -d -h $hostname --dns $2 -v $BASE_DIR:/root/app -v /Users/nanzhu/code/all-pairs-similarity/data/maildir_small:/root/data $1
    sleep 3
  done
  sleep 3
}

start_cluster codingcat/all-pairs $2
