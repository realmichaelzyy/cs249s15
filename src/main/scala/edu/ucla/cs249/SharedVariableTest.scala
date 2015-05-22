package edu.ucla.cs249

import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.ZKUtil

object SharedVariableTest {
  def main(args: Array[String]) {
    println("----- Shared Variable Test -----")
    val conf = new SharedVariableConfig(System.getenv("HDFS_ADDRESS"), System.getenv("ZK_CONNECT_STRING"))
    val zk = new ZooKeeper(System.getenv("ZK_CONNECT_STRING"), 5000, null)
    println(conf.node_path)
    println(zk.exists(conf.node_path, false))
    conf.destroy
    println(zk.exists(conf.node_path, false))
    ZKUtil.deleteRecursive(zk, "/sv")
  }
}