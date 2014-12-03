package cpslab.deploy.server

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.ConfigFactory
import cpslab.message.{DataPacket, LoadData, Test}

import scala.util.Random

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: program cluster_conf_path deploy_conf_path app_conf_path")
      sys.exit(1)
    }

    startup()

    def startup(): Unit = {

      val conf = ConfigFactory.parseFile(new File(args(0))).
        withFallback(ConfigFactory.parseFile(new File(args(1)))).
        withFallback(ConfigFactory.parseFile(new File(args(2)))).
        withFallback(ConfigFactory.load())

      println(conf.getList("akka.cluster.seed-nodes"))
      println(conf.getString("akka.persistence.journal.plugin"))
      println(conf.getString("hbase-journal.hadoop-pass-through.hbase.zookeeper.quorum"))

      val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
      val system = ActorSystem("ClusterSystem", conf)

      // fix the entry Id to send to a proxy and then spawn to the multiple entries
      // otherwise, it is impossible to send data packet to multiple entries just
      // through idExtractor
      val entryIdExtractor: ShardRegion.IdExtractor = {
        case msg => ("EntryProxy", msg)
      }

      val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
        case dp: DataPacket => dp.shardId.toString
        case ld: LoadData => Random.nextInt(maxShardNum).toString
        case p @ Test(_) => "1"//just for test
      }

      ClusterSharding(system).start(
        typeName = EntryProxyActor.entryProxyActorName,
        entryProps = Some(Props(new EntryProxyActor(conf))),
        idExtractor = entryIdExtractor,
        shardResolver = shardIdResolver
      )
    }
  }
}