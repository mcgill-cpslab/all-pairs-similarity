package cpslab.deploy

import cpslab.message.LoadData
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

object CommonUtils {
  private[deploy] def parseLoadDataRequest(tableName: String,
                                           startRowKey: Array[Byte],
                                           endRowKey: Array[Byte],
                                           maxRangeNum: Int): List[LoadData] = {
    // TODO: limit the minimum range
    val loadDataRequests = new ListBuffer[LoadData]
    val startRowKeyInt = Bytes.toLong(startRowKey)
    val endRowKeyInt = Bytes.toLong(endRowKey)
    var newStartKeyInt = startRowKeyInt
    val maxLength = (endRowKeyInt - startRowKeyInt + 1) / maxRangeNum
    while (newStartKeyInt <= endRowKeyInt) {
      val stepLength = {
        if (newStartKeyInt + maxLength > endRowKeyInt) {
          endRowKeyInt - newStartKeyInt + 1
        } else {
          maxLength
        }
      }
      val newEndKeyInt = newStartKeyInt + stepLength - 1
      loadDataRequests +=
        LoadData(tableName, Bytes.toBytes(newStartKeyInt), Bytes.toBytes(newEndKeyInt))
      newStartKeyInt = newStartKeyInt + stepLength
    }
    loadDataRequests.toList
  }
}
