package cpslab.deploy

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import akka.cluster.Cluster
import com.typesafe.config.Config
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}

import cpslab.message.{WriteWorkerFinished, DataPacket, LoadData, Message}
import cpslab.vector.{SparseVector, Vectors}

private class SimilarityWorker(workerConf: Config, localService: ActorRef) extends Actor {

  private val vectorDim = workerConf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = workerConf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = workerConf.getString("cpslab.allpair.clientPort")

  private var inputVectors: List[SparseVector] = null

  // the number of the child actors
  private val writeActorNum = workerConf.getInt("cpslab.allpair.writeActorNum")
  private val indexActorNum = workerConf.getInt("cpslab.allpair.indexActorNum")

  // children
  val indexActors = new Array[ActorRef](indexActorNum)
  val writeActors = new mutable.HashSet[ActorRef]
  // for fault-tolerance
  val writeWorkersToInputSet = new mutable.HashMap[ActorRef, List[SparseVector]]

  val cluster = Cluster(context.system)
  // listen the MemberEvent
  cluster.subscribe(self, classOf[DeadLetter])

  val messagesToStoppedActor = new mutable.HashMap[ActorRef, ListBuffer[Message]]

  private def readFromDataBase(tableName: String,
                               startRow: Array[Byte], endRow: Array[Byte]): List[SparseVector] = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val hTable = new HTable(hbaseConf, "inputTable")
    val scan = new Scan(startRow, endRow)
    scan.addFamily(Bytes.toBytes("info"))
    var retVectorArray = List[SparseVector]()
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
      retVectorArray = Vectors.sparse(vectorDim, sparseArray).asInstanceOf[SparseVector] +:
        retVectorArray
    }
    retVectorArray
  }

  override def receive: Receive = {
    case LoadData(tableName, startRow, endRow) =>
      inputVectors = readFromDataBase(tableName, startRow, endRow)
      val inputVectorLists = inputVectors.grouped(writeActorNum).toList
      for (inputList <- inputVectorLists) {
        val newWriterWorker = context.actorOf(Props(new WriteWorkerActor(workerConf,
          inputList, localService)))
        writeActors += newWriterWorker
        context.watch(newWriterWorker)
      }
    case dp @ DataPacket(key, user, vector) =>
      if (indexActors(key % indexActorNum) == null) {
        indexActors(key % indexActorNum) = context.actorOf(Props(new IndexingWorkerActor))
        context.watch(indexActors(key % indexActorNum))
      }
      indexActors(key % indexActorNum) ! dp
    case DeadLetter(message, sender, recipient) =>
      // buffer the message to any dead actor
      if (message.isInstanceOf[DataPacket] && sender == self) {
        messagesToStoppedActor.getOrElseUpdate(recipient,
          new ListBuffer[Message]) += message.asInstanceOf[DataPacket]
      }
    case WriteWorkerFinished =>
      writeActors -= sender()
      sender ! PoisonPill
    case Terminated(stoppedChild) =>
      // received from either WriteWorker or IndexingWorker
      var stoppedChildIndex = indexActors.indexOf(stoppedChild)
      if (stoppedChildIndex == -1) {
        // is a WriteWorker, restart it
        if (writeActors.contains(stoppedChild)) {
          val newWriterWorker = context.actorOf(
            Props(new WriteWorkerActor(workerConf, writeWorkersToInputSet(stoppedChild),
              localService)))
          context.watch(newWriterWorker)
          //TODO: How to de-duplicate ...can be solved by persistent actor
          writeWorkersToInputSet += newWriterWorker -> writeWorkersToInputSet(stoppedChild)
          writeWorkersToInputSet.remove(stoppedChild)
          writeActors -= stoppedChild
          writeActors += newWriterWorker
        }
      } else {
        // is a IndexingWorker
        indexActors(stoppedChildIndex) = context.actorOf(Props(new IndexingWorkerActor))
        context.watch(indexActors(stoppedChildIndex))
        //re-send the buffer data
        if (messagesToStoppedActor.contains(stoppedChild)) {
          for (msg <- messagesToStoppedActor(stoppedChild)) {
            indexActors(stoppedChildIndex) ! msg
          }
          messagesToStoppedActor.remove(stoppedChild)
        }
      }
  }
}