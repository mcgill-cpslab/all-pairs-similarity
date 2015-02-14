package cpslab.deploy.server

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import akka.actor.{Actor, ActorLogging, Cancellable}
import akka.contrib.pattern.ClusterSharding
import com.typesafe.config.Config
import cpslab.message.{DataPacket, LoadData, VectorIOMsg}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

/**
 * WriteWorker calculate the target of each vector
 * EntryProxy devolves the load to writeworker to ensure that the "relatively high time complexity
 * will not block the message processing"
 * 
 * support both reading data from hbase and receive input via akka message
 */
class WriteWorkerActor(conf: Config) extends Actor with ActorLogging {
  import context._

  private var inputVectors: List[(String, SparseVector)] = null
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = conf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = conf.getString("cpslab.allpair.clientPort")
  
  private val threshold = conf.getDouble("cpslab.allpair.indexThreshold")

  // shardId -> vectorIndex
  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[Int]] =
    new mutable.HashMap[Int, mutable.HashSet[Int]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null // parse data from HBase
  var writeTask: Cancellable = null // periodically send message to trigger IO

  var vectorsStore: ListBuffer[(String, SparseVector)] = new ListBuffer[(String, SparseVector)]

  val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
  val regionActor = ClusterSharding(context.system).shardRegion(EntryProxyActor.entryProxyActorName)
  val mode = conf.getString("cpslab.allpair.runMode")

  override def preStart(): Unit = {
    //println("starting WriteWorkerActor")
    val ioTriggerPeriod = conf.getInt("cpslab.allpair.ioTriggerPeriod")
    writeTask = context.system.scheduler.schedule(0 milliseconds,
      ioTriggerPeriod milliseconds, self, WriteWorkerActor.IOTrigger)
  }

  override def postStop(): Unit = {
    if (parseTask != null) {
      parseTask.cancel()
    }
    if (writeTask != null) {
      writeTask.cancel()
    }
  }

  private def parseInput(): Unit = {
    //println("parsing Input")
    for ((vectorId, vector) <- inputVectors){
      writeBufferLock.acquire()
      vectorsStore += (vectorId -> vector)
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % maxShardNum,// this is the shard Id
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
    parseTask.cancel()
  }

  private def doReadFromHBase(hTable: HTable, scan: Scan): List[(String, SparseVector)] = {
    val retVectorArray = new ListBuffer[(String, SparseVector)]
    try {
      for (result <- hTable.getScanner(scan).iterator()) {
        //convert to the vector
        val cells = result.rawCells()
        val sparseArray = new Array[(Int, Double)](cells.size - 1)
        var sparseArrayIndex = 0
        for (cell <- cells) {
          val qualifier = {
            if (mode == "PRODUCT") {
              Bytes.toInt(CellUtil.cloneQualifier(cell))
            } else {
              Bytes.toString(CellUtil.cloneQualifier(cell)).toInt
            }
          }
          val value = {
            if (mode == "PRODUCT") {
              Bytes.toDouble(CellUtil.cloneValue(cell))
            } else {
              Bytes.toString(CellUtil.cloneValue(cell)).toDouble
            }
          }
          // to avoid spark etl job error, we set qualifier -1 as the init element for every
          // vector
          if (qualifier != -1) {
            sparseArray(sparseArrayIndex) = qualifier -> value
            sparseArrayIndex += 1
          }
        }
        if (sparseArrayIndex != 0) {
          retVectorArray += (EntryProxyActor.nextId.getAndIncrement.toString ->
            Vectors.sparse(vectorDim, {
              if (mode == "PRODUCT") {
                sparseArray
              } else {
                sparseArray.sortWith((a, b) => a._1 < b._1)
              }
            }).asInstanceOf[SparseVector])
        }
      }
      retVectorArray.toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        retVectorArray.toList
    }
  }

  private def readFromDataBase(tableName: String,
                               startRow: Array[Byte], endRow: Array[Byte]): 
  List[(String, SparseVector)] = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val hTable = new HTable(hbaseConf, tableName)
    val scan = {
      if (mode == "PRODUCT") {
        new Scan(startRow, endRow)
      } else {
        val startRowStr = Bytes.toInt(startRow).toString
        val endRowStr = Bytes.toInt(endRow).toString
        new Scan(Bytes.toBytes(startRowStr), Bytes.toBytes(endRowStr))
      }
    }
    scan.addFamily(Bytes.toBytes("info"))
    doReadFromHBase(hTable, scan)
  }

  private def handleLoadData(loadReq: LoadData): Unit = {
    log.info("WRITEWORKERACTOR %s: received %s".format(self, loadReq))
    inputVectors = readFromDataBase(loadReq.tableName, loadReq.startRow, loadReq.endRow)
    log.info("total inputVector number:%d".format(inputVectors.size))
    parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
      def run(): Unit = {
        parseInput()
      }
    })
  }

  private def handleIOTrigger(): Unit = {
    writeBufferLock.acquire()
    if (!writeBuffer.isEmpty) {
      for ((shardId, vectors) <- writeBuffer) {
        val vectorSet = new mutable.HashSet[SparseVectorWrapper]()
        for (vectorIndexInVectorsStore <- vectors) {
          val (vectorId, sparseVector) = vectorsStore(vectorIndexInVectorsStore)
          //de-duplicate, the vector is sent to each shard for only once
          //but with all the indices which are _ % maxShardNum == shardId
          val targetIndices = sparseVector.indices.toSet.filter(_ % maxShardNum == shardId)
          if (!targetIndices.isEmpty) {
            vectorSet += SparseVectorWrapper(targetIndices, (vectorId, sparseVector))
          }
        }
        regionActor ! DataPacket(shardId, vectorSet.toSet)
      }
      writeBuffer.clear()
    }
    writeBufferLock.release()
  }

  private def handleVectorIOMsg(vectorIO: VectorIOMsg): Unit = {
    for ((vectorId, vector) <- vectorIO.vectors) {
      writeBufferLock.acquire()
      vectorsStore += (vectorId -> Vectors.sparse(vector))
      val allIndices = vector.indices
      val validIndices = allIndices.filter(i => vector.values(allIndices.indexOf(i)) >= threshold)
      for (nonZeroIdx <- validIndices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % maxShardNum, // this is the shard Id
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
  }

  override def receive: Receive = {
    case lq @ LoadData(_, _, _) =>
      handleLoadData(lq)
    case v @ VectorIOMsg(vectors) =>
      handleVectorIOMsg(v)
    case WriteWorkerActor.IOTrigger =>
      handleIOTrigger
  }
}

object WriteWorkerActor {
  case object IOTrigger
}

