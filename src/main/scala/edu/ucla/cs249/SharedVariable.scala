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
import org.apache.commons.codec.binary.Hex
import scala.collection.mutable.HashMap


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
    zk.create(node_path + "/default", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(node_path + "/dict", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
    val fsuri = URI.create (hdfs_address)
    val conf = new Configuration ()
    val fs = FileSystem.get(fsuri, conf)
    val uri = URI.create (hdfs_address + node_path)
    fs.delete(new Path(uri), true)
  }
}

class SharedVariable (conf: SharedVariableConfig) {
  /* zookeeper */
  private var zk: ZooKeeper = null
  
  /* default lock */
  private var hasDefaultLock = false
  private var defaultLock: DistributedLock = null
  
  /* dictionary locks */
  private var hasDictLock = new HashMap[String, Boolean]
  private var dictLocks = new HashMap[String, DistributedLock]
  
  
  private def ensureZK {
    if (zk == null) {
      zk = new ZooKeeper(conf.zk_connect_string, 5000, null)
    }
  }
  
  private def stringToHex(str: String) = {
    Hex.encodeHexString(str.getBytes)
  }
  
  def lock {
    ensureZK
    val itemPath = conf.node_path + "/default"
    defaultLock = new DistributedLock(zk, itemPath, "lock")
    defaultLock.lock()
    hasDefaultLock = true;
  }
  
  def lockByKey(key: String) {
    ensureZK
    val hexKey = stringToHex(key)
    val itemPath = conf.node_path + "/dict/" + hexKey
    
    /* create the node corresponding to key, if not exist */
    try {
      zk.create(itemPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch {
      case _ : Throwable => 
    }
   
    /* create lock */
    var lock = new DistributedLock(zk, itemPath, "lock")
    dictLocks.put(hexKey, lock)
    
    /* lock it */
    lock.lock()
    hasDictLock.put(hexKey, true)
  }
  
  
  def unlock {
    if (hasDefaultLock) {
      ensureZK
      defaultLock.unlock()
      defaultLock = null
      hasDefaultLock = false
    }
  }
  
  def unlockByKey(key: String) {
    val hexKey = stringToHex(key)
    if (hasDictLock.contains(hexKey)) {
      dictLocks.get(hexKey) match {
        case Some(lock: DistributedLock) => 
          lock.unlock()
          hasDictLock.remove(hexKey)
          dictLocks.remove(hexKey)
        case _ =>
      }
      
    }
  }

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