package cpslab.deploy

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.routing.ConsistentHashingRouter.ConsistentHashMapping
import akka.routing.FromConfig
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.hadoop.hbase.util.Bytes

import cpslab.message.{DataPacket, GetInputRequest}

class SimilaritySearchService(conf: Config) extends Actor {

  // programmablly define the router
 /* val workerRouter = context.actorOf(
    ClusterRouterGroup(
      ConsistentHashingGroup(List("/user/similarityService/workerRouter"),
        hashMapping = hashMapping),
      ClusterRouterGroupSettings(
        totalInstances = 100, routeesPaths = List("/user/simWorker"),
        allowLocalRoutees = true, useRole = Some("compute"))).props(),
      name = "workerRouter")*/
  val workerRouter = context.actorOf(FromConfig.props(),
    name = "workerRouter")

  val parallelism = conf.getInt("cpslab.allpair.parallelism")

  val cluster = Cluster(context.system)
  // listen the MemberEvent
  cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])

  var currentMemberNum = 0

  // TODO: set virtualNodeFactor
  val virtualNodeFactor = 1

  def hashMapping: ConsistentHashMapping = {
    case gip @ GetInputRequest(_, _, _) =>
      math.abs(gip.hashCode % (currentMemberNum * virtualNodeFactor))
    case DataPacket(key, _, _) => key
    case s: String => s
  }

  def receive: Receive = {
    case GetInputRequest(table, startRowKey, endRowKey) =>
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
        val readRequest = GetInputRequest(table, Bytes.toBytes(newStartKeyInt),
          Bytes.toBytes(newEndKeyInt))
        // TODO: to ensure the reliable delivery of the message
        // we need to either use ask pattern or use persistent to achieve at-least-once
        workerRouter ! readRequest
        newStartKeyInt = newStartKeyInt + stepLength
      }
    case dp @ DataPacket(_, _, _) =>
      // send out through the router
      workerRouter ! dp
    case MemberUp(m) if m.hasRole("compute") =>
      currentMemberNum += 1
    case other: MemberEvent =>
      currentMemberNum -= 1
    case UnreachableMember(m) =>
      currentMemberNum -= 1
    case ReachableMember(m) if m.hasRole("compute") =>
      currentMemberNum += 1
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