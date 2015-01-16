package cpslab.deploy.client

import scala.util.Random

import akka.actor.ActorSystem
import cpslab.message.VectorIOMsg
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

class ClientConnection(remoteAddress: List[String], system: ActorSystem) {

  val remoteRouter = system.actorSelection(remoteAddress(Random.nextInt(remoteAddress.length)))

  /**
   * send the set of sparseVector to remote cluster
   * @param vectors the vectors to send
   */
  def insertNewVector(vectors: Set[SparkSparseVector]): Unit = {
    remoteRouter ! VectorIOMsg(vectors)
  }
}


