import sbt.Keys._
import sbtassembly.Plugin.AssemblyKeys._

org.scalastyle.sbt.ScalastylePlugin.Settings

assemblySettings

lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.10.4",
  organization := "CodingCat",
  test in assembly :={},
  scalacOptions ++= Seq("-unchecked", "-deprecation", "-Yno-adapted-args", "-feature")
)

val commonDependency = Seq(
  ("org.scalatest" % "scalatest_2.10" % "2.2.2")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  ("org.apache.hbase" % "hbase-client" % "0.98.7-hadoop2")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  ("org.apache.hadoop" % "hadoop-common" % "2.3.0")
    .exclude ("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  ("org.apache.hbase" % "hbase-common" % "0.98.7-hadoop2")
    .exclude("commons-beanutils", "commons-beanutils-core")
)


lazy val core = (project in file("core")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      ("com.typesafe.akka" % "akka-contrib_2.10" % "2.3.6")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      ("com.typesafe" % "config" % "1.2.1")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      ("org.apache.hbase" % "hbase-server" % "0.98.7-hadoop2")
        .exclude ("org.mortbay.jetty", "servlet-api-2.5")
        .exclude ("org.mortbay.jetty", "jsp-2.1")
        .exclude ("org.mortbay.jetty", "jsp-api-2.1")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      ("org.scalanlp" % "breeze-math_2.10" % "0.4")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      ("org.apache.commons" % "commons-math3" % "3.3")
        .exclude("commons-beanutils", "commons-beanutils-core"),
      ("org.hbase" % "asynchbase" % "1.5.0")
        .exclude("org.slf4j", "log4j-over-slf4j")
        .exclude("org.slf4j", "jcl-over-slf4j")
      //("pl.project13.scala" %% "akka-persistence-hbase" % "0.4.0")
        //.exclude("org.apache.hadoop", "hadoop-core")
        //.exclude("commons-collections", "commons-collections")
    ) ++ commonDependency
  ).
  settings(assemblySettings: _*).
  settings(name := "AllPairsSimilarityCore")


lazy val etl = (project.dependsOn(core)).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-mllib_2.10" % "1.1.0",
      "org.apache.hadoop" % "hadoop-distcp" % "2.3.0",
      "org.apache.hbase" % "hbase-protocol" % "0.98.7-hadoop2",
      "org.apache.hadoop" % "hadoop-client" % "2.3.0"
    ) ++ commonDependency
  ).
  settings(name := "AllPairsSimilarityETL")

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case x => MergeStrategy.first
}
}

