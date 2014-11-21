package cpslab.message

import cpslab.vector.SparseVector

sealed trait Message

case class LoadData(tableName: String, startRow: Array[Byte], endRow: Array[Byte])
  extends Message

case class DataPacket(primaryKey: Int, vectors: Set[SparseVector])
  extends Message

case object WriteWorkerFinished extends Message
