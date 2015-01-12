#!/bin/bash

set -x

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

WORKDIR=`pwd`

SLAVES=`cat $WORKDIR/conf/slaves`

for slave in $SLAVES; do
echo $slave
ssh $SSH_OPTS $slave > /dev/null << EOF
$WORKDIR/sbin/daemon.sh start
exit
EOF
done
wait



