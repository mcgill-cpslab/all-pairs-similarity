package cpslab.deploy

import java.io.File

import scala.collection.mutable.ListBuffer
import scala.util.Random

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.server.EntryProxyActor
import cpslab.message.{DataPacket, LoadData, Test, VectorIOMsg}
import org.apache.hadoop.hbase.util.Bytes

object CommonUtils {

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          conf: Config): (Config, ActorSystem) = {
    val system = ActorSystem("ClusterSystem", conf)
    val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")

    // fix the entry Id to send to a proxy and then spawn to the multiple entries
    // otherwise, it is impossible to send data packet to multiple entries just
    // through idExtractor
    val entryIdExtractor: ShardRegion.IdExtractor = {
      case msg => ("EntryProxy", msg)
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
}
