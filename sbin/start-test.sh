#!/bin/bash

java -Xmx4096m -Xms1024m -cp core/target/scala-2.10/AllPairsSimilarityCore-assembly-0.1.jar cpslab.benchmark.LoadGenerator conf/app_benchmark.conf conf/akka_benchmark.conf 
