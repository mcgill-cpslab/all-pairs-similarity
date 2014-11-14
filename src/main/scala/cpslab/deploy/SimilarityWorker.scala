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


class WriteWorker(input: List[SparseVector], localService: ActorRef) extends Actor {

  import context._

  case object IOTrigger

  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[Int]] =
    new mutable.HashMap[Int, mutable.HashSet[Int]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null
  var writeTask: Cancellable = null

  var vectorsStore: ListBuffer[SparseVector] = new ListBuffer[SparseVector]

  override def preStart(): Unit = {
    parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
      def run(): Unit = {
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

  private def parseInput(): Unit = {
    for (vector <- input){
      vectorsStore += vector
      writeBufferLock.acquire()
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx,
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
    self ! PoisonPill
  }

  override def receive: Receive = {
    case IOTrigger =>
      writeBufferLock.acquire()
      for ((key, vectors) <- writeBuffer) {
        // TODO: userId might be discarded, need to investigate the evaluation dataset
        var vectorSet = Set[SparseVector]()
        for (vector <- vectors) {
          vectorSet += vectorsStore(vector)
        }
        // TODO: duplicate vectors may send to the same node for multiple times
        localService ! DataPacket(key, 0, vectorSet)
      }
      writeBuffer.clear()
      writeBufferLock.release()
  }
}

class IndexingWorker extends Actor {

  val cluster = Cluster(context.system)

  val vectorsStore = new ListBuffer[SparseVector]

  // dimentsionid => vector index
  val invertedIndex = new mutable.HashMap[Int, mutable.HashSet[Int]]

  def receive: Receive = {
    case DataPacket(key, uid, vectors) =>
      // save all index to the inverted index
      for (vector <- vectors) {
        vectorsStore += vector
        val currentIdx = vectorsStore.size - 1
        invertedIndex.getOrElseUpdate(key, new mutable.HashSet[Int]) += currentIdx
      }
      // TODO: output similar vectors
  }
}

class SimilarityWorker(workerConf: Config, localService: ActorRef) extends Actor {

  import SimilarityWorker._

  private val vectorDim = workerConf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = workerConf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = workerConf.getString("cpslab.allpair.clientPort")

  private var inputVectors: List[SparseVector] = null

  val writeParallelism = workerConf.getInt("cpslab.allpair.writeParallelism")
  val indexActorNum = workerConf.getInt("cpslab.allpari.indexActorNum")

  //children
  val indexActors = new Array[ActorRef](indexActorNum)
  val writeWorkersToInputSet = new mutable.HashMap[ActorRef, List[SparseVector]]

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
        val newWriterWorker = context.actorOf(Props(new WriteWorker(inputList, localService)))
        context.watch(newWriterWorker)
      }
    case dp @ DataPacket(key, user, vector) =>
      if (indexActors(key % indexActorNum) == null) {
        indexActors(key % indexActorNum) = context.actorOf(Props(new IndexingWorker))
        context.watch(indexActors(key % indexActorNum))
      }
      indexActors(key % indexActorNum) ! dp
    case MemberUp(m) if m.hasRole("compute") =>
      currentMemberNum += 1
    case other: MemberEvent =>
      currentMemberNum -= 1
    case UnreachableMember(m) =>
      currentMemberNum -= 1
    case ReachableMember(m) if m.hasRole("compute") =>
      currentMemberNum += 1
    case Terminated(stoppedChild) =>
      // received from either WriteWorker or IndexingWorker
      var stoppedChildIndex = indexActors.indexOf(stoppedChild)
      if (stoppedChildIndex == -1) {
        // is a WriteWorker, restart it
        val newWriterWorker = context.actorOf(
          Props(new WriteWorker(writeWorkersToInputSet(stoppedChild), localService)))
        context.watch(newWriterWorker)
        //TODO: How to de-duplicate
        writeWorkersToInputSet += newWriterWorker -> writeWorkersToInputSet(stoppedChild)
        writeWorkersToInputSet.remove(stoppedChild)
      } else {
        // is a IndexingWorker
        //TODO: handle cannot deliver
        indexActors(stoppedChildIndex) = context.actorOf(Props(new IndexingWorker))
        context.watch(indexActors(stoppedChildIndex))
      }
  }
}

object SimilarityWorker {
  var currentMemberNum = 1
}
