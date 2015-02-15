package cpslab.benchmark

import java.io.File

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import cpslab.message._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors}

class LoadRunner(id: Int, conf: Config) extends Actor {
  
  private var remoteActor: ActorSelection = null
  private var ioTask: Cancellable = null
  private val writeBatching = conf.getInt("cpslab.allpair.benchmark.writeBatchingDuration")
  private val totalMessageCount = conf.getInt("cpslab.allpair.benchmark.totalMessageCount")
  private var msgCount = id * totalMessageCount
  private val vectorDim = conf.getInt("cpslab.allpair.vectorDim")
  
  private val ccWebVideoLoadGenerator = new CCWEBVideoLoadGenerator(
    conf.getString("cpslab.allpair.benchmark.ccweb.path"))

  private val videos = ccWebVideoLoadGenerator.generateVectors

  private var testPhaseStarted = false
  
  private def generateVector(): Set[(String, SparkSparseVector)] = {
    val (videoId, videoFeatureVector) = videos(msgCount % videos.size)
    
    // normalize
    val squareSum = math.sqrt(videoFeatureVector.values.foldLeft(0.0)((sum, value) => sum + 
      value * value))
    val normalizedValues = videoFeatureVector.values.map(_ / squareSum)
    
    Set((msgCount.toString, Vectors.sparse(vectorDim, videoFeatureVector.indices, normalizedValues).
        asInstanceOf[SparkSparseVector]))
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
        if (!testPhaseStarted) {
          if (msgCount > videos.size && ioTask != null) {
            ioTask.cancel()
          }
        } else {
          context.parent ! StartTime(msgCount.toString, System.currentTimeMillis())
          if (msgCount > totalMessageCount && ioTask != null) {
            ioTask.cancel()
            context.stop(self)
          }
        }
        remoteActor ! VectorIOMsg(generateVector())
      }
    case StartTest =>
      println("=====Received StartTest======")
      msgCount = 0
      testPhaseStarted = true
      val system = context.system
      import system.dispatcher
      ioTask = context.system.scheduler.schedule(0 milliseconds, writeBatching milliseconds,
        self, IOTicket)
    case _ =>
  }
}

class LoadGenerator(conf: Config) extends Actor {

  private val startTime = new mutable.HashMap[String, Long]
  private val endTime = new mutable.HashMap[String, Long]
  private val findPair = new mutable.HashMap[String, mutable.HashSet[(String, Double)]]
  private val totalMessageCount = conf.getInt("cpslab.allpair.benchmark.totalMessageCount")
  private val readyVectors = new mutable.HashSet[String]
  
  private val childNum = conf.getInt("cpslab.allpair.benchmark.childrenNum")
  private val children = new mutable.HashSet[ActorRef]
  
  private val expDuration = conf.getLong("cpslab.allpair.benchmark.expDuration")
  
  private var testPhaseStarted = false
  
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
        s"time ${totalResponseTime / messageNum}")
    }
  }
  
  override def receive: Receive = {
    case similarityOutput: SimilarityOutput =>
      if (testPhaseStarted) {
        for ((queryVectorId, similarVectors) <- similarityOutput.output) {
          for ((similarVectorId, similarity) <- similarVectors) {
            val oldSize = {
              if (!findPair.contains(queryVectorId)) -1 else findPair(queryVectorId).size
            }
            findPair.getOrElseUpdate(queryVectorId, new mutable.HashSet[(String, Double)]) +=
              similarVectorId -> similarity
            val newSize = findPair(queryVectorId).size
            if (newSize != oldSize) {
              println(s"$queryVectorId -> $newSize " +
                s"lasting Time:${similarityOutput.outputMoment - startTime(queryVectorId)}")
              endTime += queryVectorId -> similarityOutput.outputMoment
            }
            if (findPair(queryVectorId).size >= totalMessageCount * childNum - 1) {
              readyVectors += queryVectorId
            }
          }
          if (readyVectors.size >= totalMessageCount * childNum) {
            context.system.shutdown()
          }
        }
      }
    case m @ StartTime(vectorId, moment) =>
      startTime += vectorId -> moment
    case ReceiveTimeout =>
      // start the test evaluation phase
      if (!testPhaseStarted) {
        testPhaseStarted = true
        for (worker <- children) {
          worker ! StartTest
        }
      } else {
        context.system.shutdown()
      }
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


