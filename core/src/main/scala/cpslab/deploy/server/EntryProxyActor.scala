package cpslab.deploy.server

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils
import cpslab.message.{Test, DataPacket, IndexData, LoadData}
import cpslab.vector.SparseVectorWrapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")
  var clientActorRef: ActorRef = null

  // the generated data structure
  // entryActorId => (SparseVectorWrapper(indices to be saved by the certain entryId,
  // sparseVectorItSelf))
  private def spawnToIndexActor(dp: DataPacket): mutable.HashMap[Int,
    mutable.ListBuffer[SparseVectorWrapper]] = {
    val writeBuffer = new mutable.HashMap[Int, mutable.ListBuffer[SparseVectorWrapper]]
    for (vectorToIndex <- dp.vectors) {
      val vectorNonZeroIndexList = vectorToIndex.indices.toList
      val indexArray = new ListBuffer[Int]
      for (i <- 0 until vectorNonZeroIndexList.length) {
        val featureIdx = vectorNonZeroIndexList(i)
        //index to different indexactors
        val indexActorIdx = featureIdx % maxIndexEntryActorNum
        writeBuffer.getOrElseUpdate(indexActorIdx, new mutable.ListBuffer[SparseVectorWrapper])
        if (i == 0) {
          writeBuffer(indexActorIdx) += SparseVectorWrapper(Set[Int](),
            vectorToIndex.sparseVector)
        }
        writeBuffer(indexActorIdx)(writeBuffer(indexActorIdx).size - 1).indices += featureIdx
      }
    }
    writeBuffer
  }

  override def receive: Receive = {
    case LoadData(tableName, startRow, endRow) =>
      clientActorRef = sender()
      val loadRequests = CommonUtils.parseLoadDataRequest(tableName, startRow, endRow,
        maxIOEntryActorNum)
      for (loadDataReq <- loadRequests) {
        val newWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf, clientActorRef)))
        newWriterWorker ! loadDataReq
        writeActors += newWriterWorker
        context.watch(newWriterWorker)
      }
    case dp @ DataPacket(_, _) =>
      for ((indexActorId, vectorsToSend) <- spawnToIndexActor(dp)) {
        if (!indexEntryActors.contains(indexActorId)) {
          val newEntryActor = context.actorOf(Props(new IndexingWorkerActor(conf, clientActorRef)))
          context.watch(newEntryActor)
          indexEntryActors += indexActorId -> newEntryActor
        }
        indexEntryActors(indexActorId) ! IndexData(vectorsToSend.toSet)
      }

    case Terminated(stoppedChild) =>
      //TODO: restart children
    case t @ Test(content) =>
      println("receiving %s".format(t))
      if (clientActorRef == null) {
        clientActorRef = sender()
      }
      val newEntryActor = context.actorOf(Props(new IndexingWorkerActor(conf, clientActorRef)))
      context.watch(newEntryActor)
      newEntryActor ! t
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
}
