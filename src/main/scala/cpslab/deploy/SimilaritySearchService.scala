package cpslab.deploy

import akka.actor.{Props, Actor}
import akka.routing.FromConfig
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes

import cpslab.message.GetInputRequest

class SimilaritySearchService(conf: Config) extends Actor {

  // the router actor
  val workerRouter = context.actorOf(FromConfig.props(Props[SimilarityWorker]),
    name = "workerRouter")

  val parallelism = conf.getInt("cpslab.allpair.parallelism")

  def receive: Receive = {
    case GetInputRequest(table, startRowKey, endRowKey) =>
      val startRowKeyInt = Bytes.toInt(startRowKey)
      val endRowKeyInt = Bytes.toInt(endRowKey)
      var newStartKeyInt = startRowKeyInt
      while (newStartKeyInt <= endRowKeyInt) {
        val stepLength = {
          if (newStartKeyInt + stepLength > endRowKeyInt) {
            endRowKeyInt - newStartKeyInt + 1
          } else {
            (endRowKeyInt - startRowKeyInt + 1) / parallelism
          }
        }
        val newEndKeyInt = newStartKeyInt + stepLength - 1
        workerRouter ! GetInputRequest(table, Bytes.toBytes(newStartKeyInt),
          Bytes.toBytes(newEndKeyInt))
        newStartKeyInt = newStartKeyInt + stepLength
        //FSM
        context.become(processing)
      }
  }

  def processing: Receive = {
    null
  }
}

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {
  }
}