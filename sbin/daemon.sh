#!/bin/bash

set -x

WORKDIR=`pwd`
PID_FILE="$WORKDIR/all-pairs-similarity/pid/sim.pid"
option=$1

STARTTIME=`date+%s%N | cut -b1-13`

case $option in
	(start)
	  mkdir -p $WORKDIR/all-pairs-similarity/pid/
		mkdir -p $WORKDIR/all-pairs-similarity/logs/
	  touch $PID_FILE
	  cd $WORKDIR/all-pairs-similarity
	  nohup java -Xmx4096m -Xms1024m -cp core/target/scala-2.10/AllPairsSimilarityCore-assembly-0.1.jar cpslab.deploy.server.SimilaritySearchService conf/akka.conf conf/app.conf > logs/similarity-$STARTTIME &
	  pid=$!
	  echo $pid > $PID_FILE
	;;

	(stop)
	  pid=`cat $WORKDIR/all-pairs-similarity/pid/sim.pid`
	  kill -9 $pid
	;;
esac
