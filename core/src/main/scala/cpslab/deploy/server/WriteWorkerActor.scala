package cpslab.deploy.server

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import akka.actor.{ActorLogging, Actor, ActorRef, Cancellable}
import akka.contrib.pattern.ClusterSharding
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

import cpslab.message.{VectorIOMsg, DataPacket, LoadData}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}

private class WriteWorkerActor(conf: Config, clientActor: ActorRef) extends Actor
with ActorLogging {
  import context._

  val clusterSharding = ClusterSharding(context.system)

  private var inputVectors: List[SparseVector] = null
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = conf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = conf.getString("cpslab.allpair.clientPort")

  // the number of the child actors
  private val writeActorNum = conf.getInt("cpslab.allpair.writeActorNum")

  // shardId -> vectorIndex
  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[Int]] =
    new mutable.HashMap[Int, mutable.HashSet[Int]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null
  var writeTask: Cancellable = null

  var vectorsStore: ListBuffer[SparseVector] = new ListBuffer[SparseVector]

  val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
  val mode = conf.getString("cpslab.allpair.runMode")

  override def preStart(): Unit = {
    println("starting WriteWorkerActor")
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
    println("parsing Input")
    for (vector <- inputVectors){
      writeBufferLock.acquire()
      vectorsStore += vector
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % maxShardNum,// this is the shard Id
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
    parseTask.cancel()
  }

  private def doReadFromHBase(hTable: HTable, scan: Scan): List[SparseVector] = {
    val retVectorArray = new ListBuffer[SparseVector]
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
          retVectorArray += Vectors.sparse(vectorDim, {
            if (mode == "PRODUCT") {
              sparseArray
            } else {
              sparseArray.sortWith((a, b) => a._1 < b._1)
            }
          }).asInstanceOf[SparseVector]
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
                               startRow: Array[Byte], endRow: Array[Byte]): List[SparseVector] = {
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

  private def handleIOTrigger: Unit = {
    writeBufferLock.acquire()
    if (!writeBuffer.isEmpty) {
      for ((shardId, vectors) <- writeBuffer) {
        val vectorSet = new mutable.HashSet[SparseVectorWrapper]()
        for (vectorIdx <- vectors) {
          val sparseVector = vectorsStore(vectorIdx)
          //de-duplicate, the vector is sent to the target actor for only once
          //but with all the indices, _ % maxShardNum == shardId
          vectorSet += SparseVectorWrapper(sparseVector.indices.toSet.
            filter(_ % maxShardNum == shardId), sparseVector)
        }
        println("sending datapacket to shardRegion actor, shardId: %d, size: %d".
          format(shardId, vectorSet.size))
        clusterSharding.shardRegion(EntryProxyActor.entryProxyActorName) !
          DataPacket(shardId, vectorSet.toSet, clientActor)
      }
      writeBuffer.clear()
    }
    writeBufferLock.release()
  }

  private def handleVectorIOMsg(vectorIO: VectorIOMsg): Unit = {
    for (vector <- vectorIO.vectors){
      writeBufferLock.acquire()
      vectorsStore += Vectors.sparse(vector)
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % maxShardNum,// this is the shard Id
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

