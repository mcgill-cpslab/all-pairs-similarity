package cpslab.etl

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import cpslab.vector.{SparseVector, Vectors}

import scala.collection.immutable.HashSet

object HBaseUpLoader {

  private def saveToHBase(config: Config, inputRDD: RDD[String], outputTable: String,
                          filterThreshold: Int ): Unit = {
    import org.apache.spark.SparkContext._
    val zooKeeperQuorum = config.getString("cpslab.cluster.hbase.zookeeperQuorum")
    val clientPort = config.getString("cpslab.cluster.hbase.clientPort")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    val vectorRDD = {
      inputRDD.zipWithIndex().map { case (vectorString, uniqueId) =>
        val putObj = new Put(Bytes.toBytes(uniqueId))
        (Vectors.parseNumeric(vectorString).asInstanceOf[SparseVector], uniqueId)
      }
    }.cache()

    //get maxWeight for each dimension
    val dimensionValueRDD = vectorRDD.flatMap{case (sparseVector, uniqueId) =>
      val res = for (i <- 0 until sparseVector.indices.length)
        yield (sparseVector.indices(i), sparseVector.values(i))
      res
    }
    val maxDimRDD = dimensionValueRDD.reduceByKey((a, b) => math.max(a, b))
    val maxDimOutRDD = maxDimRDD.map{case (index, value) => {
      val putObj = new Put(Bytes.toBytes(index))
      putObj.add(Bytes.toBytes("info"), Bytes.toBytes("maxValue"), Bytes.toBytes(value))
      (new ImmutableBytesWritable, putObj)
    }}
    val hbaseConf1 = HBaseConfiguration.create()
    hbaseConf1.set(TableOutputFormat.OUTPUT_TABLE, outputTable + "_MAX")
    hbaseConf1.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf1.set("hbase.zookeeper.property.clientPort", clientPort)
    val job1 = Job.getInstance(hbaseConf1)
    job1.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    maxDimOutRDD.saveAsNewAPIHadoopDataset(job1.getConfiguration)

    //filtering
    val sortedFeatureArray = maxDimRDD.collect().sortWith((a, b) => a._2 > b._2)
    val filteredFeatureIndexSet =
      new HashSet() ++ sortedFeatureArray.take((sortedFeatureArray.size * filterThreshold)).
        map(_._1)

    //save the HadoopDataset to HBase
    vectorRDD.map{case (sparseVector, uniqueId) =>
      val putObj = new Put(Bytes.toBytes(uniqueId))
      for (i <- 0 until sparseVector.indices.length) {
        val index = sparseVector.indices(i)
        val value = sparseVector.values(i)
        if (filteredFeatureIndexSet.contains(index)) {
          putObj.add(Bytes.toBytes("info"),
            Bytes.toBytes(index), Bytes.toBytes(value))
        }
      }
      (new ImmutableBytesWritable, putObj)
    }.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: inputDir tableName threshold")
      sys.exit(1)
    }
    val sc = new SparkContext()

    val inputFileRDD = sc.textFile(args(0))
    val config = ConfigFactory.parseString("cpslab.cluster.hbase.zookeeperQuorum=master\n" +
      "cpslab.cluster.hbase.clientPort=2181")
    saveToHBase(config, inputFileRDD, args(1), args(2).toInt)
  }
}
