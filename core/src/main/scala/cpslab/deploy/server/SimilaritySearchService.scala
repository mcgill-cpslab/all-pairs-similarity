package cpslab.deploy.server

import java.io.File

import akka.actor.Props
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.RoundRobinGroup
import com.typesafe.config.ConfigFactory
import cpslab.deploy.CommonUtils

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: program akka_conf_path app_conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0))).
      withFallback(ConfigFactory.parseFile(new File(args(1)))).
      withFallback(ConfigFactory.load())
    val (_, system) = CommonUtils.startShardingSystem(Some(Props(new EntryProxyActor(conf))),
      conf)
    val regionActorPath = ClusterSharding(system).shardRegion(EntryProxyActor.entryProxyActorName).
      path.toStringWithoutAddress
    // start the router
    system.actorOf(
      ClusterRouterGroup(RoundRobinGroup(List(regionActorPath)), ClusterRouterGroupSettings(
        totalInstances = 100, routeesPaths = List(regionActorPath),
        allowLocalRoutees = true, useRole = Some("compute"))).props(),
      name = "regionRouter")
  }
}
