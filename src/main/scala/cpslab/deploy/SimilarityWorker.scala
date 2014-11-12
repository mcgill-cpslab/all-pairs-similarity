package cpslab.deploy

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import akka.actor.Actor
import com.typesafe.config.Config
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import cpslab.vector.{SparseVector, Vectors}
import cpslab.message.{DataPacket, GetInputRequest}

class SimilarityWorker(workerConf: Config) extends Actor {

  private val vectorDim = workerConf.getInt("cpslab.allpair.vectorDim")
  private val zooKeeperQuorum = workerConf.getString("cpslab.allpair.zooKeeperQuorum")
  private val clientPort = workerConf.getString("cpslab.allpair.clientPort")

  private var inputVectors: List[SparseVector] = null

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
    case DataPacket(user, vector) =>
      //TODO: receive vectors from other actors
  }
}
