package cpslab.deploy.server

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.{implicitConversions, postfixOps}

import akka.actor.{ActorRef, Actor, Cancellable}
import akka.contrib.pattern.ClusterSharding
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

import cpslab.message.{DataPacket, LoadData, WriteWorkerFinished}
import cpslab.vector.{SparseVector, SparseVectorWrapper, Vectors}

private class WriteWorkerActor(conf: Config, clientActor: ActorRef) extends Actor {
  import context._

  val clusterSharding = ClusterSharding(context.system)

  private var inputVectors: List[SparseVector] = null
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = conf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = conf.getString("cpslab.allpair.clientPort")

  // the number of the child actors
  private val writeActorNum = conf.getInt("cpslab.allpair.writeActorNum")

  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[Int]] =
    new mutable.HashMap[Int, mutable.HashSet[Int]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null
  var writeTask: Cancellable = null

  var vectorsStore: ListBuffer[SparseVector] = new ListBuffer[SparseVector]

  val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")

  override def preStart(): Unit = {
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
    for (vector <- inputVectors){
      vectorsStore += vector
      writeBufferLock.acquire()
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % maxShardNum,// this is the shard Id
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
    parseTask.cancel()
  }

  private def readFromDataBase(tableName: String,
                               startRow: Array[Byte], endRow: Array[Byte]): List[SparseVector] = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val hTable = new HTable(hbaseConf, "inputTable")
    val scan = new Scan(startRow, endRow)
    scan.addFamily(Bytes.toBytes("info"))
    val retVectorArray = new ListBuffer[SparseVector]
    for (result <- hTable.getScanner(scan).iterator()) {
      //convert to the vector
      val cells = result.rawCells()
      val sparseArray = new Array[(Int, Double)](cells.size)
      var sparseIdx = 0
      for (cell <- cells) {
        val qualifier = Bytes.toInt(CellUtil.cloneQualifier(cell))
        val value = Bytes.toDouble(CellUtil.cloneQualifier(cell))
        sparseArray(sparseIdx) = qualifier -> value
        sparseIdx += 1
      }
      retVectorArray += Vectors.sparse(vectorDim, sparseArray).asInstanceOf[SparseVector]
    }
    retVectorArray.toList
  }

  override def receive: Receive = {
    case m @ LoadData(tableName, startRow, endRow) =>
      println("WRITEWORKERACTOR: received %s".format(m))
      inputVectors = readFromDataBase(tableName, startRow, endRow)
      println("total inputVector number:%d".format(inputVectors.size))
      parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
        def run(): Unit = {
          parseInput()
        }
      })
    case WriteWorkerActor.IOTrigger =>
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
          clusterSharding.shardRegion(EntryProxyActor.entryProxyActorName) !
            DataPacket(shardId, vectorSet.toSet)
        }
        writeBuffer.clear()
      } else {
        // send a PoisonPill after 10 seconds
        context.system.scheduler.scheduleOnce(10000 milliseconds, parent, WriteWorkerFinished)
      }
      writeBufferLock.release()
  }
}

object WriteWorkerActor {
  case object IOTrigger
}

