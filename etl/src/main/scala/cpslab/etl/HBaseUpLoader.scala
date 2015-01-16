package cpslab.etl

import java.io.File

import scala.collection.immutable.HashSet

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

object HBaseUpLoader {

  var mode: String = null
  var filteredFeatureIndexSet: HashSet[Int] = null

  private def generateMaxPutKVPair(inputPair: Option[(Int, Double)]):
    Option[(ImmutableBytesWritable, Put)] = {
    inputPair.map{case (index, maxValue) => {
      val putObj: Put = {
        if (mode == "PRODUCT") {
          val putObj = new Put(Bytes.toBytes(index))
          putObj.add(Bytes.toBytes("info"), Bytes.toBytes("maxValue"), Bytes.toBytes(maxValue))
          putObj
        } else {
          val putObj = new Put(Bytes.toBytes(index.toString))
          putObj.add(Bytes.toBytes("info"), Bytes.toBytes("maxValue"),
            Bytes.toBytes(maxValue.toString))
          putObj
        }
      }
      (new ImmutableBytesWritable, putObj)
    }}
  }

  private def generatePutKVPair(inputPair: Option[(SparseVector, Long)]):
    Option[(ImmutableBytesWritable, Put)] = {
    inputPair.map { case (sparseVector, uniqueId) =>
      val putObj = {
        if (mode == "PRODUCT") {
          new Put(Bytes.toBytes(uniqueId))
        } else {
          new Put(Bytes.toBytes(uniqueId.toString))
        }
      }
      for (i <- 0 until sparseVector.indices.length) {
        val index = sparseVector.indices(i)
        val value = sparseVector.values(i)
        if (i == 0) {
          // save the init element for every vector to avoid
          // java.lang.IllegalArgumentException: No columns to insert
          if (mode == "PRODUCT") {
            putObj.add(Bytes.toBytes("info"),
              Bytes.toBytes(-1), Bytes.toBytes(-1.0))
          } else {
            putObj.add(Bytes.toBytes("info"),
              Bytes.toBytes((-1).toString), Bytes.toBytes((-1.0).toString))
          }
        }
        if (filteredFeatureIndexSet.contains(index)) {
          if (mode == "PRODUCT") {
            putObj.add(Bytes.toBytes("info"),
              Bytes.toBytes(index), Bytes.toBytes(value))
          } else {
            putObj.add(Bytes.toBytes("info"),
              Bytes.toBytes(index.toString), Bytes.toBytes(value.toString))
          }
        }
      }
      (new ImmutableBytesWritable, putObj)
    }
  }

  /**
   * create job instance with typesafe config instance
   * @param config the input typesafe config instance
   * @return job instance to output hbase table
   */
  private def createJobInstance(config: Config, outputTable: String): Job = {
    val zooKeeperQuorum = config.getString("cpslab.allpair.zooKeeperQuorum")
    val clientPort = config.getString("cpslab.allpair.clientPort")
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
    val maxDimOutRDD = maxDimRDD.map(indexMaxValue =>
      generateMaxPutKVPair(Some(indexMaxValue)).get).
      saveAsNewAPIHadoopDataset(createJobInstance(config,
      config.getString("cpslab.allpair.rawDataTable") + "_MAX").getConfiguration)

    //filtering the unusual dimensions
    val sortedFeatureArray = maxDimRDD.collect().sortWith((a, b) => a._2 > b._2)
    filteredFeatureIndexSet =
      new HashSet[Int]() ++ sortedFeatureArray.take(filterThreshold).
        map(_._1)

    //save the HadoopDataset to HBase
    vectorRDD.map( vectorIdKVPair =>
      generatePutKVPair(Some(vectorIdKVPair)).get
    ).saveAsNewAPIHadoopDataset(createJobInstance(config,
      config.getString("cpslab.allpair.rawDataTable")).getConfiguration)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println("usage: inputDir threshold mode akka_config_path app_config_path")
      println("mode - DEBUG, PRODUCTION")
      sys.exit(1)
    }

    mode = args(2).toUpperCase

    val sc = new SparkContext()
    val inputFileRDD = sc.textFile(args(0))
    val config = ConfigFactory.parseFile(new File(args(3))).
      withFallback(ConfigFactory.parseFile(new File(args(4)))).
      withFallback(ConfigFactory.load())
    saveToHBase(config, inputFileRDD, args(1).toInt)
  }
}
