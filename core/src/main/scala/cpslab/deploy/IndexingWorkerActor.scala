package cpslab.deploy

import akka.actor.Actor
import akka.cluster.Cluster

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.message.DataPacket
import cpslab.vector.SparseVector

class IndexingWorkerActor extends Actor {
  val cluster = Cluster(context.system)

  val vectorsStore = new ListBuffer[SparseVector]

  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  def receive: Receive = {
    case DataPacket(key, uid, vectors) =>
      // save all index to the inverted index
      for (vector <- vectors) {
        vectorsStore += vector
        val currentIdx = vectorsStore.size - 1
        invertedIndex.getOrElseUpdate(key, new mutable.HashSet[Int]) += currentIdx
      }
    // TODO: output similar vectors
  }
}
