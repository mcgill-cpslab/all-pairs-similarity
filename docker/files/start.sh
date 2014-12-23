#!/bin/bash

cd /root/app;

java -Xmx2048m -Xms512m -cp /root/app/core/target/scala-2.10/AllPairsSimilarityCore-assembly-0.1.jar  cpslab.deploy.server.SimilaritySearchService conf/akka.conf conf/app.conf

while [ 1 ];
do
  sleep 3
done
