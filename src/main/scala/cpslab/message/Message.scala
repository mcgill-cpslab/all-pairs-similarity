package cpslab.message

import org.apache.spark.mllib.linalg.SparseVector

trait Message

case class GetInputRequest(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class DataPacket(user: Int, vector: SparseVector)
