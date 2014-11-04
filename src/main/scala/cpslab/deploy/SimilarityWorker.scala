package cpslab.deploy

import akka.actor.Actor
import cpslab.message.{DataPacket, GetInputRequest}

class SimilarityWorker extends Actor {

  override def receive: Receive = {
    case GetInputRequest(tableName, startRow, endRow) =>
      //TODO: use HBase API to read the input data
    case DataPacket(user, vector) =>
      //TODO: receive vectors from other actors
  }
}
