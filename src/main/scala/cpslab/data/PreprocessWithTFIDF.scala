package cpslab.data

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

object PreprocessWithTFIDF {

  /**
   * map each file specified in the allFilesPath to a single line (a string)
   * @param allFilesPath list of file path
   * @return RDD of the file content (each string per file)
   */
  def mapEachFileToSingleLine(sc: SparkContext, allFilesPath: ListBuffer[String]): RDD[String] = {
    import org.apache.spark.SparkContext._
    var allFileContentRDD: RDD[String] = null
    for (path <- allFilesPath) {
      if (allFileContentRDD == null) {
        allFileContentRDD = sc.textFile(path).map((" ", _)).reduceByKey(_ + " " + _).map(_._2)
      } else {
        allFileContentRDD = allFileContentRDD.union(
          sc.textFile(path).map((" ", _)).reduceByKey(_ + " " + _).map(_._2)
        )
      }
    }
    allFileContentRDD
  }

  def computeTFIDFVector(sc: SparkContext, documents: RDD[String]): RDD[SparkVector] = {
    val docs = documents.map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[SparkVector] = hashingTF.transform(docs)
    tf.cache()
    val idf = new IDF().fit(tf)
    idf.transform(tf)
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      println("Usage: program rootPath outPath")
      sys.exit(-1)
    }
    val sc = new SparkContext()
    val rootPath = new Path(args(0))
    val allFilesToProcess = new ListBuffer[String]
    Utils.getAllFilePath(rootPath.getFileSystem(sc.hadoopConfiguration),
      rootPath, allFilesToProcess)
    val fileContentRDD = mapEachFileToSingleLine(sc, allFilesToProcess)
    val tfidfRDD = computeTFIDFVector(sc, fileContentRDD)
    tfidfRDD.saveAsTextFile(args(1))
  }
}
