package cpslab.deploy.client

import akka.actor.{ActorSystem, ActorSelection}
import cpslab.message.VectorIOMsg
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

class ClientConnection(remoteAddress: String, system: ActorSystem) {

  val remoteRouter = system.actorSelection(remoteAddress)

  /**
   * send the set of sparseVector to remote cluster
   * @param vectors the vectors to send
   */
  def insertNewVector(vectors: Set[SparkSparseVector]): Unit = {
    remoteRouter ! VectorIOMsg(vectors)
  }
}


