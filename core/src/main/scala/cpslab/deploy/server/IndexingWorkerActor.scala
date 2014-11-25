package cpslab.deploy.server

import akka.actor.Actor
import cpslab.message.IndexData
import cpslab.vector.SparseVectorWrapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class IndexingWorkerActor extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]

  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  def receive: Receive = {
    case IndexData(vectors) =>
      for (vectorWrapper <- vectors) {
        vectorsStore += vectorWrapper
        val currentIdx = vectorsStore.length - 1
        for (nonZeroIdx <- vectorWrapper.indices) {
          invertedIndex.getOrElseUpdate(nonZeroIdx, new mutable.HashSet[Int]) += currentIdx
        }
      }
    // TODO: output similar vectors
  }
}
