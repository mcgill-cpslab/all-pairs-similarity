package cpslab.etl

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.vector.{SparseVector, Vectors}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashSet

object HBaseUpLoader {

  /**
   * create job instance with typesafe config instance
   * @param config the input typesafe config instance
   * @return job instance to output hbase table
   */
  private def createJobInstance(config: Config): Job = {
    val outputTable = config.getString("cpslab.allpair.outputTable")
    val zooKeeperQuorum = config.getString("cpslab.cluster.hbase.zookeeperQuorum")
    val clientPort = config.getString("cpslab.cluster.hbase.clientPort")
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    hbaseConf.set("hbase.zookeeper.quorum", zooKeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[Put]])
    job
  }

  /**
   * save HDFS file representing the feature vector to HBase
   * @param config the config file
   * @param inputRDD vector RDD
   * @param filterThreshold the threshold used to filter the unusual vector dimensions
   */
  private def saveToHBase(config: Config, inputRDD: RDD[String],
                          filterThreshold: Int): Unit = {
    import org.apache.spark.SparkContext._
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
    maxDimOutRDD.saveAsNewAPIHadoopDataset(createJobInstance(config).getConfiguration)

    //filtering the unusual dimensions
    val sortedFeatureArray = maxDimRDD.collect().sortWith((a, b) => a._2 > b._2)
    val filteredFeatureIndexSet =
      new HashSet[Int]() ++ sortedFeatureArray.take(filterThreshold).
        map(_._1)

    //save the HadoopDataset to HBase
    vectorRDD.map{case (sparseVector, uniqueId) =>
      val putObj = new Put(Bytes.toBytes(uniqueId))
      for (i <- 0 until sparseVector.indices.length) {
        val index = sparseVector.indices(i)
        val value = sparseVector.values(i)
        if (i == 0) {
          // save the init element for every vector to avoid
          // java.lang.IllegalArgumentException: No columns to insert
          putObj.add(Bytes.toBytes("info"),
            Bytes.toBytes(-1), Bytes.toBytes(-1.0))
        }
        if (filteredFeatureIndexSet.contains(index)) {
          putObj.add(Bytes.toBytes("info"),
            Bytes.toBytes(index), Bytes.toBytes(value))
        }
      }
      (new ImmutableBytesWritable, putObj)
    }.saveAsNewAPIHadoopDataset(createJobInstance(config).getConfiguration)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: inputDir threshold")
      sys.exit(1)
    }
    val sc = new SparkContext()

    val inputFileRDD = sc.textFile(args(0))
    val config = ConfigFactory.parseString("cpslab.cluster.hbase.zookeeperQuorum=master\n" +
      "cpslab.cluster.hbase.clientPort=2181")
    saveToHBase(config, inputFileRDD, args(1).toInt)
  }
}
