package cpslab.deploy.client

import java.io.File

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.contrib.pattern.{ShardRegion, ClusterSharding}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.CommonUtils
import cpslab.deploy.server.EntryProxyActor
import cpslab.message.{LoadData, DataPacket}
import cpslab.service.SimilaritySearchService
import org.apache.hadoop.hbase.util.Bytes

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

private class Client(config: Config) extends Actor {

  import context._

  val regionActor = ClusterSharding(context.system).shardRegion(EntryProxyActor.entryProxyActorName)

  val ioRangeNum = config.getInt("cpslab.allpair.ioRangeNum")

  val parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
    def run(): Unit = {
      terminal()
    }
  })

  override def postStop() = {
    println("stopped the client actor")
  }

  override def receive: Receive = {
    case PoisonPill =>
      if (parseTask != null) {
        parseTask.cancel()
      }
      context.stop(self)
      context.system.shutdown()
  }

  private def sendIOCommand(tableName: String, startKey: Array[Byte],
                            endKey: Array[Byte]): Unit = {
    val loadDataReqs = CommonUtils.parseLoadDataRequest(tableName, startKey, endKey, ioRangeNum)
    for (req <- loadDataReqs) {
      regionActor ! loadDataReqs
    }
  }

  private def terminal(): Unit = {
    println("Terminal:")
    var cmd = ""
    while (cmd != "quit") {
      cmd match {
        case "start" =>
          val tableName = Console.readLine()
          val startKey = Bytes.toBytes(Console.readLine().toInt)
          val endKey = Bytes.toBytes(Console.readLine().toInt)
          sendIOCommand(tableName, startKey, endKey)
      }
      cmd = Console.readLine()
    }
    self ! PoisonPill
  }
}

object Client {

  val clientActorName = "client"

  // fix the entry Id to send to a proxy and then spawn to the multiple entries
  // otherwise, it is impossible to send data packet to multiple entries just
  // through idExtractor
  val entryIdExtractor: ShardRegion.IdExtractor = {
    case msg => ("EntryProxy", msg)
  }

  val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
    case x => "1"
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

      val system = ActorSystem("ClusterSystem", conf)

      ClusterSharding(system).start(
        typeName = EntryProxyActor.entryProxyActorName,
        entryProps = None, // start the shardRegion actor in proxy mode
        idExtractor = SimilaritySearchService.entryIdExtractor,
        shardResolver = SimilaritySearchService.shardIdResolver
      )

      ClusterSharding(system).start(
        typeName = Client.clientActorName,
        entryProps = Some(Props(new Client(conf))),
        idExtractor = Client.entryIdExtractor,
        shardResolver = Client.shardIdResolver)

      system.actorOf(Props(new Client(conf)))
      system.awaitTermination()
    }
  }
}
