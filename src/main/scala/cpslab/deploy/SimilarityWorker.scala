package cpslab.deploy

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import com.typesafe.config.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import cpslab.vector.{SparseVector, Vectors}
import cpslab.message.{DataPacket, GetInputRequest}


class WriteWorker(input: List[SparseVector], localSerivce: ActorRef) extends Actor {

  import context._

  case object IOTrigger

  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[SparseVector]] =
    new mutable.HashMap[Int, mutable.HashSet[SparseVector]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null
  var writeTask: Cancellable = null

  override def preStart(): Unit = {
    parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
      def run: Unit = {
        parseInput
      }
    })
    //TODO: make the period length configurable
    writeTask = context.system.scheduler.schedule(0 milliseconds, 10 milliseconds, self, IOTrigger)
  }

  override def postStop(): Unit = {
    parseTask.cancel()
    writeTask.cancel()
  }

  private def parseInput: Unit = {
    for (vector <- input){
      writeBufferLock.acquire()
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx % SimilarityWorker.currentMemberNum,
          new mutable.HashSet[SparseVector]) += vector
      }
      writeBufferLock.release()
    }
    self ! PoisonPill
  }

  override def receive: Receive = {
    case IOTrigger =>
      writeBufferLock.acquire()
      for ((key, vectors) <- writeBuffer) {
        // TODO: userId might be discarded, need to investigate the evaluation report
        localSerivce ! DataPacket(key, 0, vectors.toSet)
      }
      writeBuffer.clear()
      writeBufferLock.release()
  }
}

class SimilarityWorker(workerConf: Config, localService: ActorRef) extends Actor {

  import SimilarityWorker._

  private val vectorDim = workerConf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = workerConf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = workerConf.getString("cpslab.allpair.clientPort")

  private var inputVectors: List[SparseVector] = null

  val writeParallelism = workerConf.getInt("cpslab.allpair.writeParallelism")

  val cluster = Cluster(context.system)
  // listen the MemberEvent
  cluster.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent])

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
    case GetInputRequest(tableName, startRow, endRow) =>
      inputVectors = readFromDataBase(tableName, startRow, endRow)
      val inputVectorLists = inputVectors.grouped(writeParallelism).toList
      for (inputList <- inputVectorLists) {
        context.actorOf(Props(new WriteWorker(inputList, localService)))
      }
    case DataPacket(key, user, vector) =>
      //TODO: receive vectors from other actors
    case MemberUp(m) if m.hasRole("compute") =>
      currentMemberNum += 1
    case other: MemberEvent =>
      currentMemberNum -= 1
    case UnreachableMember(m) =>
      currentMemberNum -= 1
    case ReachableMember(m) if m.hasRole("compute") =>
      currentMemberNum += 1
  }
}

object SimilarityWorker {
  var currentMemberNum = 1
}
