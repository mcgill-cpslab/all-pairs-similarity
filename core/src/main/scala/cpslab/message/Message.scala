package cpslab.message

import cpslab.vector.SparseVector

sealed trait Message

case class LoadData(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class DataPacket(primaryKey: Int, user: Int, vector: Set[SparseVector])
  extends Message

case object WriteWorkerFinished extends Message
