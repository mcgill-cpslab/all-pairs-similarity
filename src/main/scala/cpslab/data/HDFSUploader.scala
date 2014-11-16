package cpslab.data

import java.io._
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FileStatus}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.tools.util.DistCpUtils
import org.apache.hadoop.tools.util.ThrottledInputStream
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

object HDFSUploader {

  val prop = new Properties()
  val systemTime = System.currentTimeMillis()

  private val baseHDFSURL = prop.getProperty("cpslab.allpair.hdfs")
  private val workingDirectory = prop.getProperty(
    "cpslab.allpair.workdirectory", "temp")
  private val workTargetURL = "hdfs://%s/%s".format(baseHDFSURL, workingDirectory)
  private val backupBufferSize = 8192
  private val backupMaxRate = 8388608

  def getFileType(fileStatus: FileStatus): String = {
    if (fileStatus == null)
      "N/A"
    else {
      if (fileStatus.isDirectory)
        "dir"
      else
        "file"
    }
  }

  def checkFileType(sourceFileStatus: FileStatus, targetFileStatus: FileStatus): Boolean = {
    try {
      if (targetFileStatus != null && (targetFileStatus.isDirectory != sourceFileStatus.isDirectory)) {
        throw new IOException("Can't replace " + targetFileStatus.getPath + ". Target is " +
          getFileType(targetFileStatus) + ", Source is " + getFileType(sourceFileStatus))
      }
      true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        false
    }
  }

  def doBackup(sourceFs: FileSystem, targetFs: FileSystem, sourceFileStatus: FileStatus,
               targetPath: Path): Unit = {
    val sourcePath = sourceFileStatus.getPath
    val sourceFullPath = sourcePath.toString.substring(
      sourceFs.getUri.toString.length, sourcePath.toString.length)
    if (sourceFileStatus.isDirectory) {
      // if the source file is a directory, then we need to make the directory first
      try {
        targetFs.mkdirs(new Path(
          "%s/%s".format(workTargetURL, sourceFullPath)))
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    } else {
      val tmpTargetPath =
        new Path(workTargetURL.toString + "/" + sourceFullPath)
      // leave Progressable parameter as null
      // by reading the implementation of HDFS, I found that there is always a null guard to check
      // if progressable is null,
      // if yes, then it will never be called
      // copy to tmp file
      val outStream: OutputStream = new BufferedOutputStream(targetFs.create(
        tmpTargetPath, true, backupBufferSize.toInt,
        targetFs.getDefaultReplication(targetPath),
        targetFs.getDefaultBlockSize(targetPath), null))
      var inStream: ThrottledInputStream = null
      var totalBytesRead = 0
      try {
        inStream = new ThrottledInputStream(new BufferedInputStream(sourceFs.open(sourcePath)),
          backupMaxRate.toLong)
        val buffer = new Array[Byte](8 * 1024)
        var bytesRead = inStream.read(buffer)
        while (bytesRead >= 0) {
          totalBytesRead += bytesRead
          outStream.write(buffer, 0, bytesRead)
          bytesRead = inStream.read(buffer)
        }
      } finally {
        IOUtils.cleanup(null, outStream, inStream)
      }
      // check the legacy of the tmp files
      // first file length
      if (totalBytesRead != sourceFs.getFileStatus(sourcePath).getLen) {
        throw new IOException("Mismatch in length of source:%s and target:%s".format(sourcePath,
          targetPath.toString))
      }
      // next checksum
      // if empty file, just skip it
      if (totalBytesRead != 0) {
        if (!DistCpUtils.checksumsAreEqual(sourceFs, sourcePath, targetFs, targetPath)) {
          throw new IOException("Check-sum mismatch between %s and %s".format(sourcePath, targetPath) )
        }
      }
    }
    val targetFileStatus = targetFs.getFileStatus(targetPath)
    if (!checkFileType(sourceFileStatus, targetFileStatus)) {
      throw new Exception("file type incompatible")
    }
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: program inputPath outputPath")
      sys.exit(1)
    }
    val sc = new SparkContext()
    val rootPath = new Path(args(0))
    val allFilesToProcess = new ListBuffer[String]
    Utils.getAllDirAndFilePath(
      rootPath.getFileSystem(sc.hadoopConfiguration), rootPath, allFilesToProcess)
    prop.load(this.getClass.getClassLoader().getResourceAsStream("application.conf"))
    try {
      val pathToBackupRdd = sc.parallelize(allFilesToProcess,
        prop.getProperty("cpslab.allpair.parallelism", "10").toInt)
      pathToBackupRdd.foreach(sourcePathString => {
        val hadoopConf = new Configuration()
        val sourcePath = new Path(sourcePathString)
        val sourceFs = sourcePath.getFileSystem(hadoopConf)
        val sourceFileFullPath = sourcePath.toString.substring(
          sourceFs.getUri.toString.length, sourcePath.toString.length)
        val sourceFileStatus = sourceFs.getFileStatus(sourcePath)
        val targetPath = new Path(workTargetURL + "/" + sourceFileFullPath)
        val targetFs: FileSystem = targetPath.getFileSystem(hadoopConf)
        doBackup(sourceFs, targetFs, sourceFileStatus, targetPath)
      })
      // atomic moving
      val sourcePath = new Path(workTargetURL)
      val sourceFs = sourcePath.getFileSystem(sc.hadoopConfiguration)
      val targetPath = new Path(baseHDFSURL + "/" + args(1))
      val targetFs = targetPath.getFileSystem(sc.hadoopConfiguration)
      targetFs.rename(sourcePath, targetPath)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
}
