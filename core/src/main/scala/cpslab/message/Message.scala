package cpslab.message

import cpslab.vector.SparseVectorWrapper

sealed trait Message

case class LoadData(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

// shardId is to ensure that each vector is sent to a certain shard for only once
case class DataPacket(shardId: Int, vectors: Set[SparseVectorWrapper])
  extends Message

case class IndexData(vectors: Set[SparseVectorWrapper])

case object WriteWorkerFinished extends Message
