package edu.ucla.cs249

import org.apache.zookeeper._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import org.apache.zookeeper.data.Stat
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.Serializable
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.zookeeper.ZKUtil
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path


class SharedVariableConfig {
  var node_path: String = ""
  var hdfs_address: String = ""
  var zk_connect_string: String = ""
  def this (hdfs_address: String, zk_connect_string: String) {
    this()
    this.hdfs_address = hdfs_address
    this.zk_connect_string = zk_connect_string
    
    /* initialize zookeeper */ 
    val zk = new ZooKeeper(zk_connect_string, 5000, null)
    try {
      zk.create("/sv", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch {
      case _ : Throwable => 
    }
    val builder =  SharedInodeProto.SharedInode.newBuilder()
    builder.setNextVersion(1L)
    builder.clearExistingVersions()
    node_path = zk.create("/sv/sv", builder.build().toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
    zk.close()
    
    /* initialize hdfs */
    val fsuri = URI.create (hdfs_address)
    val conf = new Configuration ()
    val fs = FileSystem.get(fsuri, conf)
    val perm = new FsPermission("777")
    val uri = URI.create (hdfs_address + node_path)
    fs.mkdirs(new Path(uri))
    fs.setPermission(new Path(uri), perm)
    fs.close()
  }
  
  def destroy {
    /* destroy node on zookeeper */
    val zk = new ZooKeeper(zk_connect_string, 5000, null)
    try {
      ZKUtil.deleteRecursive(zk, node_path)
    } catch {
      case _ : Throwable => 
    }
    
    /* destroy node on hdfs */
//    val fsuri = URI.create (hdfs_address)
//    val conf = new Configuration ()
//    val fs = FileSystem.get(fsuri, conf)
//    val uri = URI.create (hdfs_address + node_path)
//    fs.delete(new Path(uri), true)
  }
}

class SharedVariable {

    def get() = {
      val zk = new ZooKeeper("54.88.56.9:2181", 5000, null)
      val stat = new Stat()
      val varData = zk.getData("/lockdev", false, stat)
      val bis = new ByteArrayInputStream(varData)
      val in = new ObjectInputStream(bis)
      val newobj = in.readObject()
      newobj
    }
    
    def set(newVal: Any) {
      val b = new ByteArrayOutputStream()
      val o = new ObjectOutputStream(b)
      o.writeObject(newVal)
      val byteArr = b.toByteArray()
      val zk = new ZooKeeper("54.88.56.9:2181", 5000, null)
      zk.setData("/lockdev", byteArr, -1)
    }
}