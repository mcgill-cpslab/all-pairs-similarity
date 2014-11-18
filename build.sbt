import _root_.sbtassembly.Plugin.AssemblyKeys
import _root_.sbtassembly.Plugin.AssemblyKeys._
import _root_.sbtassembly.Plugin.PathList

import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{AssemblyKeys, MergeStrategy, PathList}

org.scalastyle.sbt.ScalastylePlugin.Settings

assemblySettings

name := "AllPairsSimilarity"

version := "0.1"

scalaVersion := "2.10.4"

test in assembly :={}

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args", "-feature")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
  "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.6",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.hadoop" % "hadoop-client" % "2.3.0",
  "org.apache.hbase" % "hbase-server" % "0.98.7-hadoop2",
  "org.apache.hbase" % "hbase-client" % "0.98.7-hadoop2",
  "org.apache.hbase" % "hbase-protocol" % "0.98.7-hadoop2",
  "org.apache.hbase" % "hbase-common" % "0.98.7-hadoop2",
  "org.apache.hadoop" % "hadoop-distcp" % "2.3.0",
  "org.apache.commons" % "commons-math3" % "3.3",
  "org.scalanlp" % "breeze-math_2.10" % "0.4",
  "org.scalatest" % "scalatest_2.10" % "2.2.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}
}