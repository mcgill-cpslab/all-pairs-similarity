package cpslab.deploy.server

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, ActorRef}
import com.typesafe.config.Config
import cpslab.message.{IndexData, SimilarityOutput, Test}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}
import cpslab.deploy.CommonUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * IndexingWorkerActor indexes the real data
 *
 * NOTICE(@CodingCat) we cannot simply use EntryActor to index the data because when 
 * a vector arrives at the cluster, we have no information about its target shard (we need the 
 * functionality implemented in WriterWorkerActor
 */
private class IndexingWorkerActor(conf: Config, replyTo: Option[ActorRef]) extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  override def preRestart(reason : scala.Throwable, message : scala.Option[scala.Any]): Unit = {
    println("restarting indexActor for %s".format(reason))
  }

  private def buildInvertedIndex(candidateVectors: Set[SparseVectorWrapper]): Unit = {
    for (candidateVector <- candidateVectors) {
      //check if the vectorWrapper should be indexed
      vectorsStore += candidateVector
      val currentIdx = vectorsStore.length - 1
      for (nonZeroIdxToSaveLocally <- candidateVector.indices) {
        invertedIndex.getOrElseUpdate(nonZeroIdxToSaveLocally, new mutable.HashSet[Int]) += 
          currentIdx
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
