package cpslab.deploy.server

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils
import cpslab.message.{DataPacket, IndexData, LoadData, Test}
import cpslab.vector.SparseVectorWrapper
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")
  var clientActorRef: ActorRef = null

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

  private def readMaxWeight: mutable.HashMap[Int, Double] = {
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
        val dimensionIdx = Bytes.toInt(result.getRow)
        val maxWeight = Bytes.toDouble(CellUtil.cloneValue(cell))
        retVectorArray += dimensionIdx -> maxWeight
      }
      retVectorArray
    } catch {
      case e: Exception =>
        e.printStackTrace()
        retVectorArray
    }
  }

  override def receive: Receive = {
    case m @ LoadData(tableName, startRow, endRow) =>
      println("received %s".format(m))
      clientActorRef = sender()
      val loadRequests = CommonUtils.parseLoadDataRequest(tableName, startRow, endRow,
        maxIOEntryActorNum)
      val rand = new Random(System.currentTimeMillis())
      for (loadDataReq <- loadRequests) {
        if (writeActors.size < maxIOEntryActorNum) {
          val newWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf, clientActorRef)))
          newWriterWorker ! loadDataReq
          writeActors += newWriterWorker
          context.watch(newWriterWorker)
        } else {
          val writeActor = writeActors.toList.apply(rand.nextInt(writeActors.size))
          writeActor ! loadDataReq
        }
      }
    case dp @ DataPacket(_, _) =>
      for ((indexActorId, vectorsToSend) <- spawnToIndexActor(dp)) {
        if (!indexEntryActors.contains(indexActorId)) {
          // if we haven't read the max weight for each dimension, we should do that
          // before we start hte first indexWorker
          val newEntryActor = context.actorOf(Props(new IndexingWorkerActor(conf,
            clientActorRef, maxWeight)))
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
      val newEntryActor = context.actorOf(
        Props(new IndexingWorkerActor(conf, clientActorRef, null)))
      context.watch(newEntryActor)
      newEntryActor ! t
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
}
