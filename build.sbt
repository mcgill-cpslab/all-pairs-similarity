import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin.{MergeStrategy, PathList}

assemblySettings

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.4",
  organization := "CodingCat",
  test in assembly :={},
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args", "-feature"),
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case x if Assembly.isConfigFile(x) =>
      MergeStrategy.concat
    case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
      MergeStrategy.rename
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
  }
)

val commonDependency = Seq(
  "org.apache.spark" % "spark-mllib_2.10" % "1.1.0"
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.spark-project.akka", "akka-remote_2.10")
    exclude("org.spark-project.akka", "akka-slf4j_2.10")
    exclude("org.spark-project.akka", "akka-testkit_2.10"),
  "org.scalatest" % "scalatest_2.10" % "2.2.2",
  "org.apache.hbase" % "hbase-client" % "0.98.7-hadoop2"
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hadoop" % "hadoop-common" % "2.3.0"
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hbase" % "hbase-common" % "0.98.7-hadoop2"
    exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.hbase" % "hbase-server" % "0.98.7-hadoop2"
    exclude("org.slf4j", "slf4j-log4j12")
)

resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

lazy val core = (project in file("core")).
  settings(assemblySettings: _*).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.6"
        exclude("org.slf4j", "slf4j-log4j12"),
      "com.typesafe" % "config" % "1.2.1"
        exclude("org.slf4j", "slf4j-log4j12"),
      "org.scalanlp" % "breeze-math_2.10" % "0.4"
        exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.commons" % "commons-math3" % "3.3"
        exclude("org.slf4j", "slf4j-log4j12"),
      "org.hbase" % "asynchbase" % "1.5.0"
        exclude("org.slf4j", "slf4j-log4j12")
    ) ++ commonDependency
  ).
  settings(name := "AllPairsSimilarityCore")


lazy val etl = project.dependsOn(core).
  settings(assemblySettings: _*).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.hadoop" % "hadoop-distcp" % "2.3.0"
        exclude("org.slf4j", "slf4j-log4j12"),
      "org.apache.hadoop" % "hadoop-client" % "2.3.0"
        exclude("org.slf4j", "slf4j-log4j12")
    ) ++ commonDependency
  ).
  settings(name := "AllPairsSimilarityETL")

