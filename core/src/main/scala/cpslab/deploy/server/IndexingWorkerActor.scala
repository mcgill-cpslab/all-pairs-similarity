package cpslab.deploy.server

import akka.contrib.pattern.ClusterSharding
import cpslab.deploy.client.Client

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.Actor
import com.typesafe.config.Config
import cpslab.message.{SimilarityOutput, IndexData}
import cpslab.vector.SparseVectorWrapper

class IndexingWorkerActor(conf: Config) extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  val clientActorRegion = ClusterSharding(context.system).shardRegion(Client.clientActorName)

  def calculateSimilarity(vector1: SparseVectorWrapper, vector2: SparseVectorWrapper): Double = {
    val sparseVector1 = vector1.sparseVector
    val sparseVector2 = vector2.sparseVector
    val intersectIndex = sparseVector1.indices.intersect(sparseVector2.indices)
    var similarity = 0.0
    for (idx <- intersectIndex) {
      similarity += (sparseVector1.values(sparseVector1.indices.indexOf(idx)) *
        sparseVector2.values(sparseVector2.indices.indexOf(idx)))
    }
    similarity
  }

  private def outputSimilarItems(candidateVectors: Set[SparseVectorWrapper]):
  mutable.HashMap[SparseVectorWrapper, mutable.HashMap[SparseVectorWrapper, Double]]  = {
    val outputSimSet = new mutable.HashMap[SparseVectorWrapper,
      mutable.HashMap[SparseVectorWrapper, Double]]
    for (vectorWrapper <- candidateVectors) {
      vectorsStore += vectorWrapper
      val currentIdx = vectorsStore.length - 1
      for (nonZeroIdxToSaveLocally <- vectorWrapper.indices) {
        invertedIndex.getOrElseUpdate(nonZeroIdxToSaveLocally, new mutable.HashSet[Int]) +=
          currentIdx
        val traversingList = invertedIndex(nonZeroIdxToSaveLocally)
        for (similarVectorCandidateIdx <- traversingList) {
          val similarVectorCandidate = vectorsStore(similarVectorCandidateIdx)
          val sim = calculateSimilarity(similarVectorCandidate, vectorWrapper)
          if (sim >= similarityThreshold) {
            val condition1 = outputSimSet.contains(similarVectorCandidate) &&
              outputSimSet(similarVectorCandidate).contains(vectorWrapper)
            val condition2 = outputSimSet.contains(vectorWrapper) &&
              outputSimSet(vectorWrapper).contains(similarVectorCandidate)
            if (!condition1 && ! condition2) {
              outputSimSet.getOrElseUpdate(vectorWrapper,
                new mutable.HashMap[SparseVectorWrapper, Double]) += similarVectorCandidate -> sim
            }
          }
        }
      }
    }
    outputSimSet
  }

  def receive: Receive = {
    case IndexData(vectors) =>
      clientActorRegion ! SimilarityOutput(outputSimilarItems(vectors))
  }
}
