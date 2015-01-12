#!/bin/bash

set -x

WORKDIR=`pwd`
PID_FILE="$WORKDIR/all-pairs-similarity/pid/sim.pid"
option=$1

case $option in 
	(start)
	  mkdir -p $WORKDIR/pid/
	  touch $PID_FILE
	  cd $WORKDIR/all-pairs-similarity
	  nohup java -Xmx4096m -Xms1024m -cp core/target/scala-2.10/AllPairsSimilarityCore-assembly-0.1.jar cpslab.deploy.server.SimilaritySearchService conf/akka.conf conf/app.conf >> /dev/null &
	  pid=$!
	  echo $pid > $PID_FILE
	;;

	(stop)
	  pid=`cat $WORKDIR/all-pairs-similarity/pid/sim.pid`
	  kill -9 $pid
	;;
esac	
