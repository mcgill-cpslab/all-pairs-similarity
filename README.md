all-pairs-similarity
====================

asynchronous all pairs similarity search model 

#API#

1. class ClientConnection(remoteAddress: String, system: ActorSystem)

  1. def insertNewVector(vectors: Set[SparkSparseVector]): Unit

    Functionality: The class for the user to insert new vector to the inverted index

    Example:

```
import org.apache.spark.mllib.linalg.{SparseVector => SparkSparseVector}

// "akka.tcp://my-sys@host.example.com:5678/user/service-b" is the address of the remote router actor which passes the messages to the actors serving inverted index
// get the Spark's actorSystem by calling SparkEnv.get.actorSystem
val cc = new ClientConnection("akka.tcp://my-sys@host.example.com:5678/user/service-b", SparkEnv.get.actorSystem) 
// vector1 and vector2 are two vectors 
cc.insertNewVector(Set[SparkSparseVector](vector1, vector2))
```
