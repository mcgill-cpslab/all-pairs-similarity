package cpslab.deploy.server

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.Config
import cpslab.message.{IndexData, SimilarityOutput, Test}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private class IndexingWorkerActor(conf: Config, replyTo: ActorRef,
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

  private def checkVectorIfToBeIndexed(candidateVector: SparseVectorWrapper): Boolean = {
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
  private def querySimilarItems(candidateVectors: Set[SparseVectorWrapper]):
  mutable.HashMap[SparseVectorWrapper, mutable.HashMap[SparseVectorWrapper, Double]]  = {
    val outputSimSet = new mutable.HashMap[SparseVectorWrapper,
      mutable.HashMap[SparseVectorWrapper, Double]]
    println("candidateVectors size:%d".format(candidateVectors.size))
    for (candidateVector <- candidateVectors) {
      //check if the vectorWrapper should be indexed
      val shouldIndex = checkVectorIfToBeIndexed(candidateVector)
      var currentIdx = 0
      if (shouldIndex) {
        vectorsStore += candidateVector
        currentIdx = vectorsStore.length - 1
      }
      for (nonZeroIdxToSaveLocally <- candidateVector.indices) {
        if (shouldIndex) {
          invertedIndex.getOrElseUpdate(nonZeroIdxToSaveLocally, new mutable.HashSet[Int]) +=
            currentIdx
        }
        val similarVectors = querySimilarVectors(candidateVector,
          invertedIndex(nonZeroIdxToSaveLocally))
        // TODO: need to deduplicate
        outputSimSet.getOrElseUpdate(candidateVector,
          new mutable.HashMap[SparseVectorWrapper, Double]) ++= similarVectors
      }
    }
    outputSimSet
  }

  // get the vectors which are similar to the query vector from the given list
  private def querySimilarVectors(queryVector: SparseVectorWrapper,
                                  candidateList: mutable.HashSet[Int]):
  List[(SparseVectorWrapper, Double)] = {
    val similarityHashMap = new mutable.HashMap[SparseVectorWrapper, Double]
    // output the similar vector
    for (similarVectorCandidateIdx <- candidateList) {
      val similarVectorCandidate = vectorsStore(similarVectorCandidateIdx)
      val sim = calculateSimilarity(similarVectorCandidate, queryVector)
      if (sim >= similarityThreshold) {
        similarityHashMap += similarVectorCandidate -> sim
      }
    }
    similarityHashMap.toList
  }

  def receive: Receive = {
    case m @ IndexData(vectors) =>
      //println("INDEXWORKERACTOR: received %s".format(m))
      println("received indexdata")
      replyTo ! SimilarityOutput(querySimilarItems(vectors))
    case t @ Test(_) =>
      println("receiving %s in IndexWorkerActor, sending to %s".format(t, replyTo))
      replyTo ! t
  }
}
