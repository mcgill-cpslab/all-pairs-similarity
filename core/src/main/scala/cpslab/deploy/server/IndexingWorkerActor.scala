package cpslab.deploy.server

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, ActorSelection}
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils._
import cpslab.message.{IndexData, SimilarityOutput, Test}
import cpslab.vector.SparseVectorWrapper

/**
 * IndexingWorkerActor indexes the real data
 *
 * NOTE (@CodingCat) we cannot simply use EntryActor to index the data because when
 * a vector arrives at the cluster, we have no information about its target shard (we need the 
 * functionality implemented in WriterWorkerActor
 */
private class IndexingWorkerActor(conf: Config) extends Actor {
  val vectorsStore = new ListBuffer[SparseVectorWrapper]
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  var replyTo: Option[ActorSelection] = None

  override def preStart(): Unit = {
    replyTo = Some(context.actorSelection(conf.getString("cpslab.allpair.outputActor")))
  }
  
  override def preRestart(reason : scala.Throwable, message : scala.Option[scala.Any]): Unit = {
    reason.asInstanceOf[Exception].printStackTrace()
    if (message.isDefined) {
      println(message)
    }
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
  mutable.HashMap[String, mutable.HashMap[String, Double]]  = {

    val outputSimSet = new mutable.HashMap[String, mutable.HashMap[String, Double]]

    // get the vectors which are similar to the query vector from the given list
    def querySimilarVectors(queryVector: SparseVectorWrapper,
                            candidateList: mutable.HashSet[Int]):
    mutable.HashMap[String, Double] = {
      val queryVectorId = queryVector.sparseVector._1
      val similarityHashMap = new mutable.HashMap[String, Double]
      // output the similar vector
      for (similarVectorCandidateIdx <- candidateList) {
        val similarVectorCandidate = vectorsStore(similarVectorCandidateIdx)
        // de-duplicate the similarity calculation
        if (outputSimSet.contains(queryVectorId) &&
          !outputSimSet(queryVectorId).contains(similarVectorCandidate.sparseVector._1)) {
          val sim = calculateSimilarity(similarVectorCandidate, queryVector)
          if (sim >= similarityThreshold) {
            similarityHashMap += similarVectorCandidate.sparseVector._1 -> sim
          }
        }
      }
      similarityHashMap
    }

    for (candidateVector <- candidateVectors) {
      for (nonZeroIdxToSaveLocally <- candidateVector.indices) {
        val similarVectors = querySimilarVectors(candidateVector,
          invertedIndex(nonZeroIdxToSaveLocally))
        // TODO: need to deduplicate
        outputSimSet.getOrElseUpdate(candidateVector.sparseVector._1,
          new mutable.HashMap[String, Double]) ++= similarVectors
      }
    }
    outputSimSet
  }

  def receive: Receive = {
    case m @ IndexData(vectors) =>
      try {
        buildInvertedIndex(vectors)
        if (replyTo.isDefined) {
          //println(s"replied to client ${replyTo.get}")
          replyTo.get ! SimilarityOutput(querySimilarItems(vectors), 
            System.currentTimeMillis())
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    case t @ Test(_) =>
      println("receiving %s in IndexWorkerActor, sending to %s".format(t, replyTo))
      replyTo.get ! t
  }
}
