package cpslab.deploy.server

import java.io.File

import akka.actor.Props
import com.typesafe.config.ConfigFactory

import cpslab.deploy.CommonUtils

object SimilaritySearchService {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: program akka_conf_path app_conf_path")
      sys.exit(1)
    }

    val conf = ConfigFactory.parseFile(new File(args(0))).
      withFallback(ConfigFactory.parseFile(new File(args(1)))).
      withFallback(ConfigFactory.load())

    CommonUtils.startShardingSystem(Some(Props(new EntryProxyActor(conf))),
      conf)
  }
}
