package cpslab.deploy.client

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{Actor, PoisonPill, Props}
import akka.contrib.pattern.ClusterSharding
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes

import cpslab.deploy.CommonUtils
import cpslab.deploy.server.EntryProxyActor
import cpslab.message.{SimilarityOutput, Test}

private class Client(config: Config) extends Actor {

  import context._

  val serverRegionActor = ClusterSharding(context.system).
    shardRegion(EntryProxyActor.entryProxyActorName)

  val ioRangeNum = config.getInt("cpslab.allpair.ioRangeNum")

  val parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
    def run(): Unit = {
      terminal()
    }
  })

  override def receive: Receive = {
    case PoisonPill =>
      if (parseTask != null) {
        parseTask.cancel()
      }
      context.stop(self)
      context.system.shutdown()
    case SimilarityOutput(output) =>
      //TODO: deduplicate
      println(output.size)
    case t @ Test(x) => println(t)
    case x => println(x)
  }

  /**
   * separate the range into ioRangeNum pieces
   * @param tableName the name of the table to read data from
   * @param startKey start key value
   * @param endKey end key value
   */
  private def sendIOCommand(tableName: String, startKey: Array[Byte],
                            endKey: Array[Byte]): Unit = {
    val loadDataReqs = CommonUtils.parseLoadDataRequest(tableName, startKey, endKey, ioRangeNum)
    for (req <- loadDataReqs) {
      println("CLIENT: sending %s".format(req))
      serverRegionActor ! req
    }
  }

  private def terminal(): Unit = {
    println("Terminal:")
    var cmd = ""
    while (cmd != "quit") {
      cmd match {
        case "start" =>
          val tableName = Console.readLine()
          val startKey = Bytes.toBytes(Console.readLine().toLong)
          val endKey = Bytes.toBytes(Console.readLine().toLong)
          println("startKey: %d, endKey: %d".format(Bytes.toLong(startKey), Bytes.toLong(endKey)))
          sendIOCommand(tableName, startKey, endKey)
        case "test" =>
          val content = Console.readLine()
          println("sending %s to %s".format(content, serverRegionActor))
          serverRegionActor ! Test(content)
        case x =>
          println(x)
      }
      cmd = Console.readLine()
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

    val (conf, system) = CommonUtils.startShardingSystem(None, args(0), args(1), args(2))
    system.actorOf(Props(new Client(conf)))
  }
}
