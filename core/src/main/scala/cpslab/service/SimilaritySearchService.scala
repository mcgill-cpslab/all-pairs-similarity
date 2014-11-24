package cpslab.service

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.EntryProxyActor

import cpslab.message.DataPacket

object SimilaritySearchService {

  var maxShardNum = 0

  // fix the entry Id to send to a proxy and then spawn to the multiple entries
  // otherwise, it is impossible to send data packet to multiple entries just
  // through idExtractor
  val entryIdExtractor: ShardRegion.IdExtractor = {
    case dp: DataPacket => ("EntryProxy", dp)
  }

  val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
    case dp: DataPacket => (dp.shardId % maxShardNum).toString
  }


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

      maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
      val system = ActorSystem("ClusterSystem", conf)

      val similarityRegion = ClusterSharding(system).start(
        typeName = EntryProxyActor.entryProxyActorName,
        entryProps = Some(Props(new EntryProxyActor(conf))),
        idExtractor = entryIdExtractor,
        shardResolver = shardIdResolver
      )
    }
  }
}