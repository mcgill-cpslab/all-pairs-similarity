package cpslab.deploy

import akka.actor.{Props, Actor}
import akka.routing.FromConfig
import cpslab.message.GetInputRequest

class SimilaritySearchService extends Actor {

  // the router actor
  val workerRouter = context.actorOf(FromConfig.props(Props[SimilarityWorker]),
    name = "workerRouter")

  def receive: Receive = {
    case getRequest @ GetInputRequest(_, _, _) =>
      workerRouter ! getRequest
  }
}


object SimilaritySearchService {

  def main(args: Array[String]): Unit = {
  }
}