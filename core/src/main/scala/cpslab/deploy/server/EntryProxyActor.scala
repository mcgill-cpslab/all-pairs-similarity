package cpslab.deploy.server

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils
import cpslab.message._
import cpslab.vector.SparseVectorWrapper
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

private class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")
  val rand = new Random(System.currentTimeMillis())
  val mode = conf.getString("cpslab.allpair.runMode")

  lazy val maxWeight = readMaxWeight

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

  private def readMaxWeight: Option[mutable.HashMap[Int, Double]] = {
    val tableName = conf.getString("cpslab.allpair.rawDataTable") + "_MAX"
    val zooKeeperQuorum = conf.getString("cpslab.allpair.zooKeeperQuorum")
    val clientPort = conf.getString("cpslab.allpair.clientPort")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val hTable = new HTable(hbaseConf, tableName)
    val scan = new Scan()
    scan.addFamily(Bytes.toBytes("info"))
    val retVectorArray = new mutable.HashMap[Int, Double]
    try {
      for (result <- hTable.getScanner(scan).iterator()) {
        //convert to the vector
        val cell = result.rawCells()(0)
        val dimensionIdx = {
          if (mode == "PRODUCT") {
            Bytes.toInt(result.getRow)
          } else {
            Bytes.toString(result.getRow).toInt
          }
        }
        val maxWeight = {
          if (mode == "PRODUCT") {
            Bytes.toDouble(CellUtil.cloneValue(cell))
          } else {
            Bytes.toString(CellUtil.cloneValue(cell)).toDouble
          }
        }
        retVectorArray += dimensionIdx -> maxWeight
      }
      Some(retVectorArray)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  private def handleLoadData(loadReq: LoadData): Unit = {
    log.info("received %s".format(loadReq))
    val tableName = loadReq.tableName
    val startRow = loadReq.startRow
    val endRow = loadReq.endRow
    val loadRequests = CommonUtils.parseLoadDataRequest(tableName, startRow, endRow,
      maxIOEntryActorNum)
    for (loadDataReq <- loadRequests) {
      var targetWriterWorker: ActorRef = null
      if (writeActors.size < maxIOEntryActorNum) {
        targetWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf, Some(sender()))))
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

  private def handleVectorIOMsg(vectorIOMsg: VectorIOMsg): Unit = {
    var targetWriterWorker: ActorRef = null
    if (writeActors.size < maxIOEntryActorNum) {
      targetWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf, None)))
      writeActors += targetWriterWorker
      context.watch(targetWriterWorker)
    } else {
      targetWriterWorker = writeActors.toList.apply(rand.nextInt(writeActors.size))
    }
    if (targetWriterWorker != null) {
      targetWriterWorker ! vectorIOMsg
    }
  }

  private def handleDataPacket(dp: DataPacket): Unit = {
    for ((indexActorId, vectorsToSend) <- spawnToIndexActor(dp)) {
      if (!indexEntryActors.contains(indexActorId)) {
        // if we haven't read the max weight for each dimension, we should do that
        // before we start the first indexWorker, implemented with lazy evaluation
        // in the definition of maxWeight
        val newEntryActor = context.actorOf(Props(new IndexingWorkerActor(conf,
          dp.clientActor, maxWeight)))
        context.watch(newEntryActor)
        indexEntryActors += indexActorId -> newEntryActor
      }
      indexEntryActors(indexActorId) ! IndexData(vectorsToSend.toSet)
    }
  }

  override def receive: Receive = {
    case m @ LoadData(tableName, startRow, endRow) =>
      handleLoadData(m)
    case v @ VectorIOMsg(vectors) =>
      handleVectorIOMsg(v)
    case dp @ DataPacket(_, _, clientActor) =>
      handleDataPacket(dp)
    case Terminated(stoppedChild) =>
      //TODO: restart children
    case t @ Test(content) =>
      println("receiving %s".format(t))
      val newEntryActor = context.actorOf(
        Props(new IndexingWorkerActor(conf, Some(sender()), null)))
      context.watch(newEntryActor)
      newEntryActor ! t
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
  
  val nextId = new AtomicInteger(0)
}
