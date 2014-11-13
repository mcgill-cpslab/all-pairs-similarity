package cpslab.message

import cpslab.vector.SparseVector

trait Message

case class GetInputRequest(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class DataPacket(key: Int, user: Int, vector: Set[SparseVector])
