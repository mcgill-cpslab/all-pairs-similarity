package cpslab.vector

/**
 * this class is used for sending the sparse vector in the cluster
 * @param indices telling the shardSupervisor about the specified keys (the nonzero index in the
 *                  vector in binary case) of the vector to be indexed
 * @param sparseVector the real vector data
 */
case class SparseVectorWrapper(var indices: Set[Int], sparseVector: (String, SparseVector)) {
  override def toString : String = {
    sparseVector._1
  }
}
