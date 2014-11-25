package cpslab.deploy.client

import java.io.File
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{PoisonPill, Actor, Props, ActorSystem}
import akka.contrib.pattern.ClusterSharding
import com.typesafe.config.{Config, ConfigFactory}

import cpslab.deploy.server.EntryProxyActor
import cpslab.service.SimilaritySearchService

private class Client(config: Config) extends Actor {

  import context._

  val regionActor = ClusterSharding(context.system).shardRegion(EntryProxyActor.entryProxyActorName)

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

  private def terminal(): Unit = {
    println("Terminal:")
    var cmd = ""
    while (cmd != "quit") {
      cmd = Console.readLine()
      println("input is %s".format(cmd))
    }
    self ! PoisonPill
  }
}

object Client {

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
        entryProps = Some(Props(new EntryProxyActor(conf))),
        idExtractor = SimilaritySearchService.entryIdExtractor,
        shardResolver = SimilaritySearchService.shardIdResolver
      )

      system.actorOf(Props(new Client(conf)))
      system.awaitTermination()
    }
  }
}
