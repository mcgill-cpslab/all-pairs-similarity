package cpslab.benchmark

import java.io.File

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.message._
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors}

class LoadRunner(id: Int, conf: Config) extends Actor {
  
  private var remoteActor: ActorSelection = null
  private var ioTask: Cancellable = null
  private val writeBatching = conf.getInt("cpslab.allpair.benchmark.writeBatchingDuration")
  private val totalMessageCount = conf.getInt("cpslab.allpair.benchmark.totalMessageCount")
  private var msgCount = id * totalMessageCount
  private val sparseFactor = conf.getDouble("cpslab.allpair.sparseFactor")
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  
  private def generateVector(): Set[(String, SparkSparseVector)] = {
    var currentIdx = 0
    val idxValuePairs = Seq.fill(vectorDim)(
    {
      currentIdx += 1
      val retPair = {
        if (Random.nextDouble() < sparseFactor) {
          (currentIdx - 1, Random.nextDouble())
        } else {
          (currentIdx - 1, 0.0)
        }
      }
      retPair
    }
    ).filter{case (index, value) => value != 0.0}
    
    // normalize
    val squareSum = math.sqrt(idxValuePairs.foldLeft(0.0)((sum, pair) => sum + pair._2 * pair._2))
    val normalizedIdxValues = idxValuePairs.map{case (index, value) => (index, value / squareSum)}
    
    Set((msgCount.toString, 
      Vectors.sparse(vectorDim, normalizedIdxValues).asInstanceOf[SparkSparseVector]))
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
  }
  
  override def receive: Receive = {
    case IOTicket =>
      if (remoteActor != null) {
        msgCount += 1
        remoteActor ! VectorIOMsg(generateVector())
        context.parent ! StartTime(msgCount.toString, System.currentTimeMillis())
      }
      if (msgCount >= totalMessageCount * (id + 1) && ioTask != null) {
        ioTask.cancel()
        context.stop(self)
      }
    case _ =>
  }
}

class LoadGenerator(conf: Config) extends Actor {

  private val startTime = new mutable.HashMap[String, Long]
  private val endTime = new mutable.HashMap[String, Long]
  
  private var totalStartTime = 0L
  private var totalEndTime = 0L

  private val childNum = conf.getInt("cpslab.allpair.benchmark.childrenNum")
  private val children = new mutable.HashSet[ActorRef]
  
  private val outputFilter = conf.getStringList("cpslab.allpair.benchmark.outputFilter")

  private val expDuration = conf.getLong("cpslab.allpair.benchmark.expDuration")
  
  if (expDuration > 0) {
    context.setReceiveTimeout(expDuration milliseconds)
  }
  
  override def preStart(): Unit = {
    for (i <- 0 until childNum) {
      children += context.actorOf(Props(new LoadRunner(i, conf)))
    }
  }

  override def postStop(): Unit = {
    val messageNum = endTime.size
    var totalResponseTime = 0L
    for ((vectorId, startMoment) <- startTime if endTime.contains(vectorId)) {
      totalResponseTime += endTime(vectorId) - startMoment
    }
    if (messageNum > 0) {
      println(s"$self stopped with $messageNum messages, average response " +
        s"time ${totalResponseTime / messageNum} totalTime: ${totalEndTime - totalStartTime}")
    }
  }
  
  override def receive: Receive = {
    case similarityOutput: SimilarityOutput =>
      if (totalStartTime == 0) {
        totalStartTime = System.currentTimeMillis()
      }
      for ((queryVectorId, similarVectors) <- similarityOutput.output) {
        for ((similarVectorId, similarity) <- similarVectors) {
          println(s"$queryVectorId -> $similarVectorId ($similarity)")
        }
        endTime += queryVectorId -> similarityOutput.outputMoment
        totalEndTime = math.max(totalEndTime, similarityOutput.outputMoment)
      }
    case m @ StartTime(vectorId, moment) =>
      startTime += vectorId -> moment
    case ReceiveTimeout =>
      context.stop(self)
    case _ =>
  }
}

object LoadGenerator {
  
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0))).withFallback(
      ConfigFactory.parseFile(new File(args(1)))).withFallback(ConfigFactory.load())
    val system = ActorSystem("LoadGenerator", conf)
    system.actorOf(Props(new LoadGenerator(conf)), name = "LoadGenerator")
    system.awaitTermination()
  }
}


