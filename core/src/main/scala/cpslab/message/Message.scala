package cpslab.message

import scala.collection.mutable

import akka.actor.ActorRef
import cpslab.vector.SparseVectorWrapper
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

sealed trait Message

case class LoadData(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class VectorIOMsg(vectors: Set[(String, SparkSparseVector)]) extends Message

// shardId is to ensure that each vector is sent to a certain shard for only once
case class DataPacket(shardId: Int, vectors: Set[SparseVectorWrapper],
                      clientActor: Option[ActorRef]) extends Message

case class IndexData(vectors: Set[SparseVectorWrapper])

case class SimilarityOutput(output: mutable.HashMap[String, mutable.HashMap[String, Double]]) {

  override def toString: String = {
    val outputStringBuilder = new mutable.StringBuilder()
    for (similarityPairResult <- output) {
      outputStringBuilder.append("---------------------------------")
      outputStringBuilder.append(similarityPairResult._1 + ":")
      for ((similarVector, similarity) <- similarityPairResult._2) {
        outputStringBuilder.append(similarVector + "," + similarity + ";")
      }
      outputStringBuilder.append("\n")
    }
    outputStringBuilder.toString()
  }
}

case class Test(content: String) extends Message

case object IOTicket
