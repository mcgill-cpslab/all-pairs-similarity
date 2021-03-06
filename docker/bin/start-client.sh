#!/bin/bash

BASE_DIR=`pwd`


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

function start_client() {
  echo "starting all-pairs cluster"

  docker run -t -i -h compute-client --dns $2 -v $BASE_DIR:/root/app -v /Users/nanzhu/code/all-pairs-similarity/data/maildir_small:/root/data -v /Users/nanzhu/code/spark:/root/spark $1 /bin/bash

  sleep 3
}

start_client codingcat/all-pairs $1
