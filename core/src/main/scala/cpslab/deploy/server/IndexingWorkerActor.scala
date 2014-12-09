package cpslab.deploy.server

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.Config
import cpslab.message.{IndexData, SimilarityOutput, Test}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class IndexingWorkerActor(conf: Config, replyTo: ActorRef,
                          maxWeightMap: mutable.HashMap[Int, Double]) extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  //assuming the normalized vectors
  private def calculateSimilarity(vector1: SparseVectorWrapper,
                                  vector2: SparseVectorWrapper): Double = {
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

  private def checkVectorIfShouldbeIndexed(candidateVector: SparseVectorWrapper): Boolean = {
    val maxWeightedVector = {
      val keys = candidateVector.sparseVector.indices
      val values = maxWeightMap.filter{case (key, value) => keys.contains(key)}.values.toArray
      SparseVectorWrapper(candidateVector.indices,
        Vectors.sparse(keys.size, keys, values).asInstanceOf[SparseVector])
    }
    calculateSimilarity(maxWeightedVector, candidateVector) >= similarityThreshold
  }

  override def preRestart(reason : scala.Throwable, message : scala.Option[scala.Any]): Unit = {
    println("restarting indexActor for %s".format(reason))
  }

  // build the inverted index with the given SparseVectorWrapper
  private def outputSimilarItems(candidateVectors: Set[SparseVectorWrapper]):
  mutable.HashMap[SparseVectorWrapper, mutable.HashMap[SparseVectorWrapper, Double]]  = {
    val outputSimSet = new mutable.HashMap[SparseVectorWrapper,
      mutable.HashMap[SparseVectorWrapper, Double]]
    println("candidateVectors size:%d".format(candidateVectors.size))
    for (vectorWrapper <- candidateVectors) {
      //check if the vectorWrapper should be indexed
      val shouldIndex = checkVectorIfShouldbeIndexed(vectorWrapper)
      var currentIdx = 0
      if (shouldIndex) {
        vectorsStore += vectorWrapper
        currentIdx = vectorsStore.length - 1
      }
      for (nonZeroIdxToSaveLocally <- vectorWrapper.indices) {
        if (shouldIndex) {
          invertedIndex.getOrElseUpdate(nonZeroIdxToSaveLocally, new mutable.HashSet[Int]) +=
            currentIdx
        }
        val traversingList = invertedIndex(nonZeroIdxToSaveLocally)
        //output the similar vector
        for (similarVectorCandidateIdx <- traversingList) {
          val similarVectorCandidate = vectorsStore(similarVectorCandidateIdx)
          val sim = calculateSimilarity(similarVectorCandidate, vectorWrapper)
          if (sim >= similarityThreshold) {
            // deduplicate
            val condition1 = outputSimSet.contains(similarVectorCandidate) &&
              outputSimSet(similarVectorCandidate).contains(vectorWrapper)
            val condition2 = outputSimSet.contains(vectorWrapper) &&
              outputSimSet(vectorWrapper).contains(similarVectorCandidate)
            if (!condition1 && !condition2) {
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
    case m @ IndexData(vectors) =>
      //println("INDEXWORKERACTOR: received %s".format(m))
      println("received indexdata")
      replyTo ! SimilarityOutput(outputSimilarItems(vectors))
    case t @ Test(_) =>
      println("receiving %s in IndexWorkerActor, sending to %s".format(t, replyTo))
      replyTo ! t
  }
}
