package cpslab.message

import scala.collection.mutable

import akka.actor.ActorRef
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

import cpslab.vector.{SparseVector, SparseVectorWrapper}

sealed trait Message

case class LoadData(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class VectorIOMsg(vectors: Set[SparkSparseVector]) extends Message

// shardId is to ensure that each vector is sent to a certain shard for only once
case class DataPacket(shardId: Int, vectors: Set[SparseVectorWrapper],
                      clientActor: Option[ActorRef]) extends Message

case class IndexData(vectors: Set[SparseVectorWrapper])

case class SimilarityOutput(output: mutable.HashMap[SparseVectorWrapper,
  mutable.HashMap[SparseVectorWrapper, Double]])

case object WriteWorkerFinished extends Message

case class Test(content: String) extends Message
