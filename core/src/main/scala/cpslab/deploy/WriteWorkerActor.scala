package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock
import scala.concurrent.duration._
import scala.language.postfixOps

import com.typesafe.config.Config

import akka.actor.{ActorRef, Cancellable, Actor}
import cpslab.message.{WriteWorkerFinished, DataPacket}
import cpslab.vector.SparseVector

private class WriteWorkerActor(conf: Config, input: List[SparseVector],
                               localService: ActorRef) extends Actor {
  import context._

  case object IOTrigger

  val writeBuffer: mutable.HashMap[Int, mutable.HashSet[Int]] =
    new mutable.HashMap[Int, mutable.HashSet[Int]]

  val writeBufferLock: Lock = new Lock

  var parseTask: Cancellable = null
  var writeTask: Cancellable = null

  var vectorsStore: ListBuffer[SparseVector] = new ListBuffer[SparseVector]

  val maxKeyRangeNum = conf.getInt("cpslab.allpair.maxKeyRangeNum")

  override def preStart(): Unit = {
    parseTask = context.system.scheduler.scheduleOnce(0 milliseconds, new Runnable {
      def run(): Unit = {
        parseInput()
      }
    })
    val ioTriggerPeriod = conf.getInt("cpslab.allpair.ioTriggerPeriod")
    writeTask = context.system.scheduler.schedule(0 milliseconds,
      ioTriggerPeriod milliseconds, self, IOTrigger)
  }

  override def postStop(): Unit = {
    parseTask.cancel()
    writeTask.cancel()
  }

  private def parseInput(): Unit = {
    for (vector <- input){
      vectorsStore += vector
      writeBufferLock.acquire()
      for (nonZeroIdx <- vector.indices) {
        writeBuffer.getOrElseUpdate(
          nonZeroIdx,
          new mutable.HashSet[Int]) += vectorsStore.size - 1
      }
      writeBufferLock.release()
    }
    parseTask.cancel()
  }

  override def receive: Receive = {
    case IOTrigger =>
      writeBufferLock.acquire()
      if (!writeBuffer.isEmpty) {
        for ((key, vectors) <- writeBuffer) {
          // TODO: userId might be discarded, need to investigate the evaluation dataset
          var vectorSet = Set[SparseVector]()
          for (vector <- vectors) {
            vectorSet += vectorsStore(vector)
          }
          // TODO: duplicate vectors may send to the same node for multiple times
          localService ! DataPacket(key, 0, vectorSet)
        }
        writeBuffer.clear()
      } else {
        // send a PoisonPill after 10 seconds
        context.system.scheduler.scheduleOnce(10000 milliseconds, parent, WriteWorkerFinished)
      }
      writeBufferLock.release()
  }
}
