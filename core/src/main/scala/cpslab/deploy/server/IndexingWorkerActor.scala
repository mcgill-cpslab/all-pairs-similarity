package cpslab.deploy.server

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.Config
import cpslab.message.{IndexData, SimilarityOutput, Test}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

private class IndexingWorkerActor(conf: Config, replyTo: Option[ActorRef],
                          maxWeightMap: Option[mutable.HashMap[Int, Double]]) extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  //assuming the normalized vectors
  private def calculateSimilarity(vector1: SparseVectorWrapper,
                                  vector2: SparseVectorWrapper): Double = {
    val (vectorId1, sparseVector1) = vector1.sparseVector
    val (vectorId2, sparseVector2) = vector2.sparseVector
    val intersectIndex = sparseVector1.indices.intersect(sparseVector2.indices)
    var similarity = 0.0
    for (idx <- intersectIndex) {
      similarity += (sparseVector1.values(sparseVector1.indices.indexOf(idx)) *
        sparseVector2.values(sparseVector2.indices.indexOf(idx)))
    }
    similarity
  }

  private def checkVectorIfToBeIndexed(candidateVector: SparseVectorWrapper): Boolean = {
    if (maxWeightMap.isDefined) {
      val maxWeightedVector = {
        val keys = candidateVector.sparseVector._2.indices
        val values = maxWeightMap.map(
          _.filter {
            case (key, value) => keys.contains(key)
          }.values.toArray).get
        SparseVectorWrapper(candidateVector.indices,
          ("maxVector", Vectors.sparse(keys.size, keys, values).asInstanceOf[SparseVector]))
      }
      calculateSimilarity(maxWeightedVector, candidateVector) >= similarityThreshold
    } else {
      true
    }
  }

  override def preRestart(reason : scala.Throwable, message : scala.Option[scala.Any]): Unit = {
    println("restarting indexActor for %s".format(reason))
  }

  private def buildInvertedIndex(candidateVectors: Set[SparseVectorWrapper]): Unit = {
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
      }
    }
  }

  // build the inverted index with the given SparseVectorWrapper
  private def querySimilarItems(candidateVectors: Set[SparseVectorWrapper]):
  mutable.HashMap[SparseVectorWrapper, mutable.HashMap[SparseVectorWrapper, Double]]  = {

    // get the vectors which are similar to the query vector from the given list
    def querySimilarVectors(queryVector: SparseVectorWrapper,
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

    val outputSimSet = new mutable.HashMap[SparseVectorWrapper,
      mutable.HashMap[SparseVectorWrapper, Double]]
    println("candidateVectors size:%d".format(candidateVectors.size))
    for (candidateVector <- candidateVectors) {
      for (nonZeroIdxToSaveLocally <- candidateVector.indices) {
        val similarVectors = querySimilarVectors(candidateVector,
          invertedIndex(nonZeroIdxToSaveLocally))
        // TODO: need to deduplicate
        outputSimSet.getOrElseUpdate(candidateVector,
          new mutable.HashMap[SparseVectorWrapper, Double]) ++= similarVectors
      }
    }
    outputSimSet
  }

  def receive: Receive = {
    case m @ IndexData(vectors) =>
      //println("INDEXWORKERACTOR: received %s".format(m))
      println("received indexdata")
      buildInvertedIndex(vectors)
      if (replyTo.isDefined) {
        replyTo.get ! SimilarityOutput(querySimilarItems(vectors))
      } else {
        //TODO: regulate the output (HDFS does not allow append)
        val hadoopConf = new Configuration()
        hadoopConf.set("fs.default.name", conf.getString("cpslab.allpair.hdfs"))
        val fs = FileSystem.get(hadoopConf)
        val fos = fs.create(new Path("/output_" + System.currentTimeMillis()))
        fos.writeChars(SimilarityOutput(querySimilarItems(vectors)).toString)
        fos.close()
      }
    case t @ Test(_) =>
      println("receiving %s in IndexWorkerActor, sending to %s".format(t, replyTo))
      replyTo.get ! t
  }
}
