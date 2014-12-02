package cpslab.etl

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector => SparkVector}
import org.apache.spark.rdd.RDD

object PreprocessWithTFIDF {

  /**
   * map each file specified in the allFilesPath to a single line (a string)
   * @param allFilesPath list of file path
   * @return RDD of the file content (each string per file)
   */
  def mapEachFileToSingleLine(sc: SparkContext, allFilesPath: ListBuffer[String]): RDD[String] = {
    var allFileContentRDD: RDD[String] = null
    val allFilesPathRDD = sc.parallelize(allFilesPath, 4)
    allFileContentRDD = allFilesPathRDD.map(sourcePathString =>  {
      val hadoopConf = new Configuration()
      val sourcePath = new Path(sourcePathString)
      val sourceFs = sourcePath.getFileSystem(hadoopConf)
      val sourceFileFullPath = sourcePath.toString.substring(
        sourceFs.getUri.toString.length, sourcePath.toString.length)
      val sourceFileStatus = sourceFs.getFileStatus(sourcePath)
      val br = new BufferedReader(new InputStreamReader(sourceFs.open(sourcePath)))
      var retStr = ""
      var line = ""
      while (line != null) {
        line = br.readLine()
        retStr += (line + " ")
      }
      retStr
    })
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
