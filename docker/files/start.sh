#!/bin/bash

cd app;

java -cp core/target/scala-2.10/AllPairsSimilarityCore-assembly-0.1.jar cpslab.service.SimilaritySearchService conf/cluster.conf conf/deploy.conf conf/app.conf

while [ 1 ];
do
  sleep 3
done
