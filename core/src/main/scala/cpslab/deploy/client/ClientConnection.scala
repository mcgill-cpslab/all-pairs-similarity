package cpslab.deploy.client

import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor.ActorSystem
import cpslab.message.VectorIOMsg
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

class ClientConnection(remoteAddresses: List[String], localActorSystem: ActorSystem) {
  
  val remoteAddressRouterList = {
    val brokerList = new ListBuffer[String]
    for (hostPortPair <- remoteAddresses) {
      val (remoteHost, remotePort) = {
        val hostAndPort = hostPortPair.split(":")
        (hostAndPort(0), hostAndPort(1))
      }
      brokerList += s"akka.tcp://ClusterSystem@$remoteHost:$remotePort/user/regionRouter"
    }
    brokerList.toList
  }

  val remoteRouter = localActorSystem.actorSelection(remoteAddresses(
    Random.nextInt(remoteAddresses.length)))

  /**
   * send the set of sparseVector to remote cluster
   * @param vectors the vectors to send
   */
  def insertNewVector(vectors: Set[SparkSparseVector]): Unit = {
    remoteRouter ! VectorIOMsg(vectors)
  }
}


