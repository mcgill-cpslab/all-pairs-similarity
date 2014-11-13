package cpslab.deploy

import akka.actor.{Props, Actor}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.routing.ConsistentHashingRouter.{ConsistentHashMapping, ConsistentHashableEnvelope}
import akka.routing.{ConsistentHashingGroup, FromConfig}
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes

import cpslab.message.GetInputRequest

class SimilaritySearchService(conf: Config) extends Actor {

  // programmablly define the router
  val workerRouter = context.actorOf(
    ClusterRouterGroup(
      ConsistentHashingGroup(List("/user/simWorker"), hashMapping = hashMapping),
      ClusterRouterGroupSettings(
        totalInstances = 100, routeesPaths = List("/user/simWorker"),
        allowLocalRoutees = true, useRole = Some("compute"))).props(),
      name = "workerRouter")

  val parallelism = conf.getInt("cpslab.allpair.parallelism")

  val cluster = Cluster(context.system)
  // listen the MemberEvent
  cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])

  var currentMemberNum = 0

  val virtualNodeFactor = math.max(context.system.settings.config.getInt(
    "akka.actor.deployment./similarity/workerRouter/.virtual-nodes-factor"), 1)

  def hashMapping: ConsistentHashMapping = {
    case gip @ GetInputRequest(_, _, _) =>
      math.abs(gip.hashCode % (currentMemberNum * virtualNodeFactor))
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
        workerRouter ! ConsistentHashableEnvelope(readRequest, readRequest.hashCode())
        newStartKeyInt = newStartKeyInt + stepLength
      }
    case MemberUp(m) if m.hasRole("compute") =>
      currentMemberNum += 1
    case other: MemberEvent =>
      currentMemberNum -= 1
    case UnreachableMember(m) =>
      currentMemberNum -= 1
    case ReachableMember(m) if m.hasRole("compute") =>
      currentMemberNum += 1
  }

  def processing: Receive = {
    null
  }
}

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {

    //start both SimilaritySearchService and SimilarityWorker
  }
}