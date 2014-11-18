package cpslab.etl

import java.io.{File, IOException}

import com.typesafe.config.{ConfigFactory, Config}
import cpslab.vector.{SparseVector, Vectors}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object HBaseUpLoader {

  private def saveToHBase(config: Config, inputRDD: RDD[String], outputTable: String): Unit = {
    import org.apache.spark.SparkContext._
    val zooKeeperQuorum = config.getString("cpslab.cluster.hbase.zookeeperQuorum")
    val clientPort = config.getString("cpslab.cluster.hbase.clientPort")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    inputRDD.zipWithUniqueId().map{case (vectorString, uniqueId) =>
      val putObj = new Put(Bytes.toBytes(uniqueId))
      val featureVector = Vectors.parseNumeric(vectorString).asInstanceOf[SparseVector]
      for (i <- 0 until featureVector.indices.length) {
        putObj.add(Bytes.toBytes("info"),
          Bytes.toBytes(featureVector.indices(i)), Bytes.toBytes(featureVector.values(i)))
      }
      (new ImmutableBytesWritable, putObj)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("usage: inputDir tableName")
      sys.exit(1)
    }

    val sc = new SparkContext()
    val inputFileRDD = sc.textFile(args(0))
    val config = ConfigFactory.parseString("cpslab.cluster.hbase.zookeeperQuorum=master\n" +
      "cpslab.cluster.hbase.clientPort=2181")
    saveToHBase(config, inputFileRDD, args(1))
  }
}
