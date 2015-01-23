package cpslab.benchmark

import java.io.File

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.Random
import scala.concurrent.duration._

import akka.actor._
import com.typesafe.config.{ConfigFactory, Config}
import cpslab.message.{SimilarityOutput, VectorIOMsg, IOTicket}
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors}

class LoadRunner(conf: Config) extends Actor {
  
  private var remoteActor: ActorSelection = null
  private var ioTask: Cancellable = null
  private var msgCount = 0
  
  private val writeBatching = conf.getInt("cpslab.allpair.benchmark.writeBatchingDuration")
  private val totalMessageCount = conf.getInt("cpslab.allpair.benchmark.totalMessageCount")
  private val sparseFactor = conf.getDouble("cpslab.allpair.sparseFactor")
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  
  private val startTime = new mutable.HashMap[String, Long]
  private val endTime = new mutable.HashMap[String, Long]
  private val expDuration = conf.getLong("cpslab.allpair.benchmark.expDuration")
  
  context.setReceiveTimeout(expDuration milliseconds)
  
  private def generateVector(): Set[(String, SparkSparseVector)] = {
    var currentIdx = 0
    val idxValuePairs = Seq.fill(vectorDim)(
    {
      currentIdx += 1
      val retPair = {
        if (Random.nextDouble() < sparseFactor) {
          (currentIdx - 1, 1.0)
        } else {
          (currentIdx - 1, 0.0)
        }
      }
      retPair
    }
    ).filter{case (index, value) => value != 0.0}
    Set((msgCount.toString, 
      Vectors.sparse(vectorDim, idxValuePairs).asInstanceOf[SparkSparseVector]))
  }

  override def preStart(): Unit = {
    val system = context.system
    import system.dispatcher
    ioTask = context.system.scheduler.schedule(0 milliseconds, writeBatching milliseconds, 
      self, IOTicket)
    remoteActor = context.actorSelection(
      conf.getString("cpslab.allpair.benchmark.remoteTarget"))
  }
  
  override def postStop(): Unit = {
    if (ioTask != null) {
      ioTask.cancel()
    }
    var totalResponseTime = 0L
    for ((vectorId, startMoment) <- startTime) {
      totalResponseTime += endTime(vectorId) - startMoment  
    }
    println(s"$self stopped with $msgCount messages, average response " +
      s"time ${totalResponseTime / totalMessageCount}")
  }
  
  override def receive: Receive = {
    case IOTicket =>
      if (remoteActor != null) {
        msgCount += 1
        startTime += msgCount.toString -> System.currentTimeMillis()
        remoteActor ! VectorIOMsg(generateVector())
      }
    case output: SimilarityOutput =>
      val receivedMoment = System.currentTimeMillis()
      for ((queryVectorId, similarVectors) <- output.output) {
        println(s"received output for vector $queryVectorId")
        endTime += queryVectorId -> receivedMoment
      }
    case _ =>   
  }
}

class LoadGenerator(conf: Config) extends Actor {
  
  private val childNum = conf.getInt("cpslab.allpair.benchmark.childrenNum")
  
  override def preStart(): Unit = {
    for (i <- 0 until childNum) {
      context.actorOf(Props(new LoadRunner(conf)))
    }
  }
  
  override def receive: Receive = {
    case _ =>
  }
}

object LoadGenerator {
  
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0))).withFallback(
      ConfigFactory.parseFile(new File(args(1)))).withFallback(ConfigFactory.load())
    val system = ActorSystem("LoadGenerator", conf)
    system.actorOf(Props(new LoadGenerator(conf)))
    system.awaitTermination()
  }
}


