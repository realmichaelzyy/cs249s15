package edu.ucla.cs249

import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZKUtil

class TestThread(conf: SharedVariableConfig, name: String) extends Runnable {
  def run {
    var shared = new SharedVariable(conf)
    println("before lock " + name)
    shared.lockByKey("abc")
    println("before sleep " + name)
    Thread.sleep(10000)
    println("after sleep " + name)
    shared.unlockByKey("abc")
    println("after lock " + name)
  }
}

class TestObject(symbol: String, value: BigDecimal) extends Serializable {
  override def toString = f"$symbol%s is ${value.toDouble}%.2f"
}

object SharedVariableTest {
  def main(args: Array[String]) {
    println("----- Shared Variable Test -----")
    val conf = new SharedVariableConfig(System.getenv("HDFS_ADDRESS"), System.getenv("ZK_CONNECT_STRING"))
    val zk = new ZooKeeper(System.getenv("ZK_CONNECT_STRING"), 5000, null)
    println(conf.node_path)
    println(zk.exists(conf.node_path, false))
    
    /*val thr1 = new Thread(new TestThread(conf, "1"))
    thr1.start
    val thr2 = new Thread(new TestThread(conf, "2"))
    thr2.start
    thr1.join
    thr2.join*/
    
    var shared = new SharedVariable(conf)
    val obj = new TestObject("ABC", 12.34)
    shared.setByKey("a", obj)
    //shared.set(new TestObject("DEF", 56.78))
    println(shared.getByKey("a"))
    conf.destroy
    println(zk.exists(conf.node_path, false))

  }
}