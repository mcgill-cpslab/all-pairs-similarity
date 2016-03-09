package cpslab.benchmark

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector, Vectors}

class CCWEBVideoLoadGenerator(path: String) {
  
  private def lineParser(line: String): (String, SparkSparseVector) = {
    val removeParenthesis = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
    val propertyArray = removeParenthesis.split(",")
    val videoID = propertyArray(0)
    val vectorSize = propertyArray(1).toInt
    val allIndices = (0 until vectorSize).toArray
    val allValues = propertyArray.takeRight(vectorSize).map(_.toDouble)
    val filteredIndices = allIndices.filter(allValues(_) != 0)
    val filteredValues = allValues.filter(_ != 0)
    (videoID, Vectors.sparse(vectorSize, filteredIndices, filteredValues).
      asInstanceOf[SparkSparseVector])
  }
  
  def generateVectors: List[(String, SparkSparseVector)] = {
    val lb = new ListBuffer[(String, SparkSparseVector)]
    for(line <- Source.fromFile(path).getLines()) {
      lb += lineParser(line)
    }
    lb.toList
  }
}
