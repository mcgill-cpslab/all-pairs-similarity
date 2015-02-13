package cpslab.deploy.server

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils._
import cpslab.message._
import cpslab.vector.SparseVectorWrapper
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors => SparkVectors}

private class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")
  val rand = new Random(System.currentTimeMillis())
  val mode = conf.getString("cpslab.allpair.runMode")
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")
  val vectorDim = conf.getInt("cpslab.allpair.vectorDim")

  lazy val maxWeightMap = readMaxWeight
  
  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    println(s"$self restarted")
    reason.printStackTrace()
  }

  // the generated data structure
  // indexWorkerId => (SparseVectorWrapper(indices to be saved by the certain entryId,
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

  private def readMaxWeight: Option[mutable.HashMap[Int, Double]] = {
    val ret = new mutable.HashMap[Int, Double]
    for (i <- 0 until vectorDim) {
      ret += (i -> 1.0)
    }
    Some(ret)
  }

  private def handleLoadData(loadReq: LoadData): Unit = {
    log.info("received %s".format(loadReq))
    val tableName = loadReq.tableName
    val startRow = loadReq.startRow
    val endRow = loadReq.endRow
    val loadRequests = parseLoadDataRequest(tableName, startRow, endRow,
      maxIOEntryActorNum)
    for (loadDataReq <- loadRequests) {
      var targetWriterWorker: ActorRef = null
      if (writeActors.size < maxIOEntryActorNum) {
        targetWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf)))
        writeActors += targetWriterWorker
        context.watch(targetWriterWorker)
      } else {
        targetWriterWorker = writeActors.toList.apply(rand.nextInt(writeActors.size))
      }
      if (targetWriterWorker != null) {
        targetWriterWorker ! loadDataReq
      }
    }
  }

  private def checkIfVectorToBeIndexed(candidateVector: SparkSparseVector): Boolean = {
    if (maxWeightMap.isDefined) {
      val maxWeightedVector = {
        val keys = candidateVector.indices
        val values = maxWeightMap.get.filter{case (key, value) => keys.contains(key)}
          .values.toArray
        SparkVectors.sparse(vectorDim, keys, values).asInstanceOf[SparkSparseVector]
      }
      calculateSimilarity(maxWeightedVector, candidateVector) >= similarityThreshold
    } else {
      true
    }
  }

  private def handleVectorIOMsg(vectorIOMsg: VectorIOMsg): Unit = {
    var targetWriterWorker: ActorRef = null
    val newVectorsSet = vectorIOMsg.vectors.filter(a => checkIfVectorToBeIndexed(a._2))
    if (newVectorsSet.size > 0) {
      val validVectors = VectorIOMsg(newVectorsSet)
      if (writeActors.size < maxIOEntryActorNum) {
        targetWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf)))
        writeActors += targetWriterWorker
        context.watch(targetWriterWorker)
      } else {
        targetWriterWorker = writeActors.toList(rand.nextInt(writeActors.size))
      }
      if (targetWriterWorker != null) {
        targetWriterWorker ! validVectors
      }
    }
  }

  private def handleDataPacket(dp: DataPacket): Unit = {
    for ((indexActorId, vectorsToSend) <- spawnToIndexActor(dp)) {
      if (!indexEntryActors.contains(indexActorId)) {
        val newIndexActor = context.actorOf(Props(new IndexingWorkerActor(conf)))
        context.watch(newIndexActor)
        indexEntryActors += indexActorId -> newIndexActor
      }
      indexEntryActors(indexActorId) ! IndexData(vectorsToSend.toSet)
    }
  }

  override def receive: Receive = {
    case m @ LoadData(tableName, startRow, endRow) =>
      handleLoadData(m)
    case v @ VectorIOMsg(vectors) =>
      handleVectorIOMsg(v)
    case dp: DataPacket =>
      handleDataPacket(dp)
    case Terminated(stoppedChild) =>
      //TODO: restart children
    case t @ Test(content) =>
      println("receiving %s".format(t))
      val newEntryActor = context.actorOf(Props(new IndexingWorkerActor(conf)))
      context.watch(newEntryActor)
      newEntryActor ! t
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
  
  val nextId = new AtomicInteger(0)
}
