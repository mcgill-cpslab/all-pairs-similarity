#!/bin/bash

BASE_DIR=`pwd`

SPARK_HOME=/Users/nanzhu/code/spark

export PATH=$SPARK_HOME/bin:$PATH

function start_client() {
  echo "starting client container"
  docker run -t -i --dns $3 -h client -v $SPARK_HOME:/root/spark -v $BASE_DIR/target/scala-2.10/:/root/app -v /Users/nanzhu/code/all-pairs-similarity/data/maildir_small:/root/data -v  /Users/nanzhu/code/docker/fs/hadoop-2.3.0:/root/hadoop-2.3.0 -v /Users/nanzhu/code/docker/fs/hadoop:/mnt/sda1/hadoop -v /Users/nanzhu/code/docker/fs/hbase-0.98.7:/root/hbase-0.98.7 $1:$2 /bin/bash

  sleep 3
}

start_client codingcat/dev basic $1
