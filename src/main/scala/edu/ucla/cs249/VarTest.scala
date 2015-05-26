package edu.ucla.cs249

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import org.apache.zookeeper.ZooKeeper
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

class SerObj() extends Serializable {
  var arr = new ArrayBuffer[Int]
  var dict = new HashMap[Int, String]
  var num = 0
  var num2 = 0
}

object VarTest {
  def main(args: Array[String]) {
    println("----- VarTest -----")
//    var myi = new SparkConcurrentVariable
//    var obj = new SerObj("name", 123)
//    myi.set(obj)
//    val zk = new ZooKeeper(System.getenv("ZK_CONNECCT_STRING"), 5000, null)
//    val lock = new DistributedLock(zk, "/abc", "def");
//    lock.lock()
//    lock.unlock()
//    
//    var obj = myi.get()
//    obj match {
//      case serobj: SerObj => 
//        println(serobj.getname)
//        println(serobj.getid)
//    } 
    println(System.getenv("ZK_CONNECT_STRING"))
    val conf = new SharedVariableConfig(System.getenv("HDFS_ADDRESS"), System.getenv("ZK_CONNECT_STRING"))
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(conf)
  }
}