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
      for (i <- 0 until maxIndexEntryActorNum) {
        val allIndices = vectorToIndex.indices.filter(_ % maxIndexEntryActorNum == i)
        writeBuffer.getOrElseUpdate(i, new ListBuffer[SparseVectorWrapper]) +=
          SparseVectorWrapper(allIndices, vectorToIndex.sparseVector)
      }
    }
    writeBuffer
  }

  override def receive: Receive = {
    case m @ LoadData(tableName, startRow, endRow) =>
      println("received %s".format(m))
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
