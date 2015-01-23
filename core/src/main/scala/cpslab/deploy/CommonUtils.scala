package cpslab.deploy

import java.io.File

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.server.EntryProxyActor
import cpslab.message.{DataPacket, LoadData, Test, VectorIOMsg}
import cpslab.vector.SparseVectorWrapper
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors}
import org.apache.hadoop.hbase.util.Bytes

object CommonUtils {

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          conf: Config): (Config, ActorSystem) = {
    val system = ActorSystem("ClusterSystem", conf)
    val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
    val maxEntryNum = conf.getInt("cpslab.allpair.maxEntryNum")
    // fix the entry Id to send to a proxy and then spawn to the multiple entries
    // otherwise, it is impossible to send data packet to multiple entries just
    // through idExtractor
    val entryIdExtractor: ShardRegion.IdExtractor = {
      // except DataPacket, it does not matter which shard the message is sent to 
      // DataPacket is related to shard, so for a given shardId, we need to ensure that it is 
      // always sent to the correct indexWorkerActor
      case dp: DataPacket=> ((dp.shardId % maxShardNum).toString, dp)
      case msg => (Random.nextInt(maxEntryNum).toString, msg)
    }
    val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
      case dp: DataPacket => dp.shardId.toString
      case ld: LoadData => Random.nextInt(maxShardNum + 1).toString
      case p @ Test(_) => "1"//just for test
      case v: VectorIOMsg => Random.nextInt(maxShardNum + 1).toString
    }
    ClusterSharding(system).start(
      typeName = EntryProxyActor.entryProxyActorName,
      entryProps = entryProps, // start the server shardRegion actor in proxy mode
      idExtractor = entryIdExtractor,
      shardResolver = shardIdResolver
    )
    (conf, system)
  }

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          akkaConfPath: String,
                                          appConfPath: String): (Config, ActorSystem) = {

    val conf = ConfigFactory.parseFile(new File(akkaConfPath)).
      withFallback(ConfigFactory.parseFile(new File(appConfPath))).
      withFallback(ConfigFactory.load())

    startShardingSystem(entryProps, conf)
  }

  private[deploy] def parseLoadDataRequest(tableName: String,
                                           startRowKey: Array[Byte],
                                           endRowKey: Array[Byte],
                                           maxRangeNum: Int): List[LoadData] = {
    // TODO: limit the minimum range
    val loadDataRequests = new ListBuffer[LoadData]
    val startRowKeyInt = Bytes.toInt(startRowKey)
    val endRowKeyInt = Bytes.toInt(endRowKey)
    var newStartKeyInt = startRowKeyInt
    val maxLength = (endRowKeyInt - startRowKeyInt + 1) / maxRangeNum
    while (newStartKeyInt <= endRowKeyInt) {
      val stepLength = {
        if (newStartKeyInt + maxLength > endRowKeyInt) {
          endRowKeyInt - newStartKeyInt + 1
        } else {
          maxLength
        }
      }
      val newEndKeyInt = newStartKeyInt + stepLength - 1
      loadDataRequests +=
        LoadData(tableName, Bytes.toBytes(newStartKeyInt), Bytes.toBytes(newEndKeyInt))
      newStartKeyInt = newStartKeyInt + stepLength
    }
    loadDataRequests.toList
  }


  //assuming the normalized vectors
  // TODO: move to a proper package as this is not related to deploy
  def calculateSimilarity(vector1: SparseVectorWrapper, vector2: SparseVectorWrapper): Double = {
    val (_, sparseVector1) = vector1.sparseVector
    val (_, sparseVector2) = vector2.sparseVector
    calculateSimilarity(
      new SparkSparseVector(sparseVector1.size, sparseVector1.indices, sparseVector1.values),
      new SparkSparseVector(sparseVector2.size, sparseVector2.indices, sparseVector2.values))
  }

  def calculateSimilarity(vector1: SparkSparseVector, vector2: SparkSparseVector): Double = {
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val vector1Map = new mutable.HashMap[Int, Double]
    val vector2Map = new mutable.HashMap[Int, Double]
    for (i <- 0 until vector1.indices.size) {
      vector1Map += vector1.indices(i) -> vector1.values(i)
    }
    for (i <- 0 until vector2.indices.size) {
      vector2Map += vector2.indices(i) -> vector2.values(i)
    }
    for ((idx, value) <- vector1Map) {
      similarity += {
        if (vector2Map.contains(idx)) value * vector2Map(idx)
        else 0.0
      }  
    }
    similarity
  }
}
