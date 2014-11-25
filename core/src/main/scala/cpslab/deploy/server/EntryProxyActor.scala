package cpslab.deploy.server

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils
import cpslab.message.{DataPacket, IndexData, LoadData}
import cpslab.vector.SparseVectorWrapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")

  // the generated data structure
  // entryActorId => (SparseVectorWrapper(indices to be saved by the certain entryId,
  // sparseVectorItSelf))
  private def spawnToEntries(dp: DataPacket): mutable.HashMap[Int,
    mutable.ListBuffer[SparseVectorWrapper]] = {
    val writeBuffer = new mutable.HashMap[Int, mutable.ListBuffer[SparseVectorWrapper]]
    for (vectorToIndex <- dp.vectors) {
      val vectorNonZeroIndexList = vectorToIndex.indices.toList
      val indexArray = new ListBuffer[Int]
      for (i <- 0 until vectorNonZeroIndexList.length) {
        val featureIdx = vectorNonZeroIndexList(i)
        val entryKey = featureIdx % maxIndexEntryActorNum
        writeBuffer.getOrElseUpdate(entryKey, new mutable.ListBuffer[SparseVectorWrapper])
        if (i == 0) {
          writeBuffer(entryKey) += SparseVectorWrapper(Set[Int](),
            vectorToIndex.sparseVector)
        }
        writeBuffer(entryKey)(writeBuffer(entryKey).size - 1).indices += featureIdx
      }
    }
    writeBuffer
  }

  override def receive: Receive = {
    case LoadData(tableName, startRow, endRow) =>
      val loadRequests = CommonUtils.parseLoadDataRequest(tableName, startRow, endRow,
        maxIOEntryActorNum)
      for (loadDataReq <- loadRequests) {
        val newWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf)))
        newWriterWorker ! loadDataReq
        writeActors += newWriterWorker
        context.watch(newWriterWorker)
      }
    case dp @ DataPacket(_, _) =>
      for ((entryActorId, vectorsToSend) <- spawnToEntries(dp)) {
        if (indexEntryActors.contains(entryActorId)) {
          val newEntryActor = context.actorOf(Props(new IndexingWorkerActor))
          context.watch(newEntryActor)
          indexEntryActors += entryActorId -> newEntryActor
        }
        indexEntryActors(entryActorId) ! IndexData(vectorsToSend.toSet)
      }

    case Terminated(stoppedChild) =>
      //TODO: restart children
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
}
