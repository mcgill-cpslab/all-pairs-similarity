import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

org.scalastyle.sbt.ScalastylePlugin.Settings

assemblySettings

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.4",
  organization := "CodingCat",
  test in assembly :={},
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args", "-feature"),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case PathList("META-INF", xs@_*) =>
      (xs map {
        _.toLowerCase
      }) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case _ => MergeStrategy.discard
      }
    case x => MergeStrategy.first
  }
  }
)

val commonDependency = Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.2",
  "org.apache.hbase" % "hbase-client" % "0.98.7-hadoop2",
  "org.apache.hadoop" % "hadoop-common" % "2.3.0",
  "org.apache.hbase" % "hbase-common" % "0.98.7-hadoop2",
  "org.apache.hbase" % "hbase-server" % "0.98.7-hadoop2"
)


lazy val core = (project in file("core")).
  settings(assemblySettings: _*).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependency ++ Seq(
      "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.6",
      "com.typesafe" % "config" % "1.2.1",
      "org.scalanlp" % "breeze-math_2.10" % "0.4",
      "org.apache.commons" % "commons-math3" % "3.3",
      "org.hbase" % "asynchbase" % "1.5.0"
    )
  ).
  settings(name := "AllPairsSimilarityCore")


lazy val etl = project.dependsOn(core).
  settings(assemblySettings: _*).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependency ++ Seq(
      "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
      "org.apache.hadoop" % "hadoop-distcp" % "2.3.0",
      "org.apache.hadoop" % "hadoop-client" % "2.3.0",
      "com.google.protobuf" % "protobuf-java" % "2.5.0"
    ),
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
      case PathList("META-INF", xs@_*) =>
        (xs map {
          _.toLowerCase
        }) match {
          case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
            MergeStrategy.discard
          case _ => MergeStrategy.discard
        }
      case x => MergeStrategy.last
    }
    }
  ).
  settings(name := "AllPairsSimilarityETL")

