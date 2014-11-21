package cpslab.deploy

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.{ConsistentHashingGroup, FromConfig}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.hadoop.hbase.util.Bytes

import cpslab.message.{DataPacket, LoadData}

class SimilaritySearchService(conf: Config) extends Actor {

  // programmablly define the router
  val workerRouter = context.actorOf(
    ClusterRouterGroup(
      ConsistentHashingGroup(List("/user/similarityService/workerRouter"),
        hashMapping = hashMapping),
      ClusterRouterGroupSettings(
        totalInstances = 100, routeesPaths = List("/user/simWorker"),
        allowLocalRoutees = true, useRole = Some("compute"))).props(),
      name = "workerRouter")

  val parallelism = conf.getInt("cpslab.allpair.parallelism")

  private def hashMapping: ConsistentHashMapping = {
    case DataPacket(key, _) => math.abs(key)
    case x => x.toString
  }

  private def parseLoadDataRequest(tableName: String, startRowKey: Array[Byte],
                           endRowKey: Array[Byte]): List[LoadData] = {
    var loadDataRequests = List[LoadData]()
    val startRowKeyInt = Bytes.toInt(startRowKey)
    val endRowKeyInt = Bytes.toInt(endRowKey)
    var newStartKeyInt = startRowKeyInt
    var maxLength = (endRowKeyInt - startRowKeyInt + 1) / parallelism
    while (newStartKeyInt <= endRowKeyInt) {
      val stepLength = {
        if (newStartKeyInt + maxLength > endRowKeyInt) {
          endRowKeyInt - newStartKeyInt + 1
        } else {
          (endRowKeyInt - startRowKeyInt + 1) / parallelism
        }
      }
      val newEndKeyInt = newStartKeyInt + stepLength - 1
      loadDataRequests =
        LoadData(tableName, Bytes.toBytes(newStartKeyInt), Bytes.toBytes(newEndKeyInt)) +:
          loadDataRequests
      newStartKeyInt = newStartKeyInt + stepLength
    }
    loadDataRequests
  }

  def receive: Receive = {
    case LoadData(table, startRowKey, endRowKey) =>
      for (getInputRequest <- parseLoadDataRequest(table, startRowKey, endRowKey)) {
        workerRouter ! getInputRequest
      }
    case dp @ DataPacket(_, _) =>
      // send out through the router
      workerRouter ! dp
  }
}

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: program cluster_conf_path deploy_conf_path app_conf_path")
      sys.exit(1)
    }

    startup()

    def startup(): Unit = {
      // Override the configuration of the port when specified as program argument
      val conf = ConfigFactory.load("application.conf").
        withFallback(ConfigFactory.parseFile(new File(args(0)))).
        withFallback(ConfigFactory.parseFile(new File(args(1)))).
        withFallback(ConfigFactory.parseFile(new File(args(2))))
      val system = ActorSystem("ClusterSystem", conf)

      val serviceActor = system.actorOf(
        Props(new SimilaritySearchService(conf)), name = "similarityService")
      system.actorOf(Props(new SimilarityWorker(conf, serviceActor)), name = "simWorker")
    }
  }
}