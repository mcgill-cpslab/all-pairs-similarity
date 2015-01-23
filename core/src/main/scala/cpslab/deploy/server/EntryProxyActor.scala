package cpslab.deploy.server

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.CommonUtils._
import cpslab.message._
import cpslab.vector.{SparseVector, Vectors, SparseVectorWrapper}
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

private class EntryProxyActor(conf: Config) extends Actor with ActorLogging  {

  val writeActors = new mutable.HashSet[ActorRef]
  val maxIOEntryActorNum = conf.getInt("cpslab.allpair.maxIOEntryActorNum")
  val indexEntryActors = new mutable.HashMap[Int, ActorRef]
  val maxIndexEntryActorNum = conf.getInt("cpslab.allpair.maxIndexEntryActorNum")
  val rand = new Random(System.currentTimeMillis())
  val mode = conf.getString("cpslab.allpair.runMode")
  val similarityThreshold = conf.getDouble("cpslab.allpair.similarityThreshold")

  lazy val maxWeightMap = readMaxWeight

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
        val ret = new mutable.HashMap[Int, Double]
        for (i <- 0 until conf.getInt("cpslab.allpairs.vectorDim")) {
          ret += (i -> 1.0)
        }
        Some(ret)
    }
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

  private def checkIfVectorToBeIndexed(candidateVector: SparkSparseVector): Boolean = {
    if (maxWeightMap.isDefined) {
      val maxWeightedVector = {
        val keys = candidateVector.indices
        val values = maxWeightMap.map(_.filter {case (key, value) => keys.contains(key)}.
          values.toArray).get
        Vectors.sparse(keys.size, keys, values).asInstanceOf[SparkSparseVector]
      }
      calculateSimilarity(maxWeightedVector, candidateVector) >= similarityThreshold
    } else {
      true
    }
  }

  private def handleVectorIOMsg(vectorIOMsg: VectorIOMsg): Unit = {
    var targetWriterWorker: ActorRef = null
    val newVectorsSet = vectorIOMsg.vectors.filter{
      case (_, vector) => checkIfVectorToBeIndexed(vector)}
    val validVectors = VectorIOMsg(newVectorsSet)
    if (writeActors.size < maxIOEntryActorNum) {
      targetWriterWorker = context.actorOf(Props(new WriteWorkerActor(conf, Some(sender()))))
      writeActors += targetWriterWorker
      context.watch(targetWriterWorker)
    } else {
      targetWriterWorker = writeActors.toList.apply(rand.nextInt(writeActors.size))
    }
    if (targetWriterWorker != null) {
      targetWriterWorker ! validVectors
    }
  }

  private def handleDataPacket(dp: DataPacket): Unit = {
    for ((indexActorId, vectorsToSend) <- spawnToIndexActor(dp)) {
      if (!indexEntryActors.contains(indexActorId)) {
        val newIndexActor = context.actorOf(Props(new IndexingWorkerActor(conf,
          dp.clientActor)))
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
    case dp @ DataPacket(_, _, clientActor) =>
      handleDataPacket(dp)
    case Terminated(stoppedChild) =>
      //TODO: restart children
    case t @ Test(content) =>
      println("receiving %s".format(t))
      val newEntryActor = context.actorOf(
        Props(new IndexingWorkerActor(conf, Some(sender()))))
      context.watch(newEntryActor)
      newEntryActor ! t
  }
}

object EntryProxyActor {
  val entryProxyActorName = "entryProxy"
  
  val nextId = new AtomicInteger(0)
}
