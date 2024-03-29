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


class SharedVariableConfig extends Serializable {
  var node_path: String = ""
  var hdfs_address: String = ""
  var zk_connect_string: String = ""
  def this (hdfs_address: String, zk_connect_string: String) {
    this()
    this.hdfs_address = hdfs_address
    this.zk_connect_string = zk_connect_string
    
    /* initialize zookeeper */ 
    val watch = new Watcher() {
      override def process(event: WatchedEvent) {}
    }
    val zk = new ZooKeeper(zk_connect_string, 5000, watch)
    try {
      zk.create("/sv", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch {
      case _ : Throwable => 
    }
    val builder =  SharedInodeProto.SharedInode.newBuilder()
    builder.setNextVersion(1L)
    builder.clearReads()
    node_path = zk.create("/sv/sv", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL)
    zk.create(node_path + "/default", builder.build().toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(node_path + "/dict", builder.build().toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
  
  /* key related */
  private var byKey = false
  private var _key = "default"
  private var keyPath = "/default"
  
  private def ensureZK {
    if (zk == null) {
      zk = new ZooKeeper(conf.zk_connect_string, 5000, null)
    }
  }
  
  private def stringToHex(str: String) = {
    Hex.encodeHexString(str.getBytes)
  }
  
  def lock {
    if (!hasDefaultLock) {
      ensureZK
      val itemPath = conf.node_path + "/default"
      defaultLock = new DistributedLock(zk, itemPath, "lock")
      defaultLock.lock()
      hasDefaultLock = true;
    }
  }
  
  def lockByKey(key: String) {
    ensureZK
    val hexKey = stringToHex(key)
    val itemPath = conf.node_path + "/dict/" + hexKey
    
    /* create the node corresponding to key, if not exist */
    try {
      val builder =  SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(1L)
      builder.clearReads()
      zk.create(itemPath, builder.build().toByteArray(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

  private def hasLock() : Boolean = {
    if (byKey && hasDictLock.contains(stringToHex(_key)))
      return true
    else if (!byKey && hasDefaultLock)
      return true
    else
      return false
  }
  
  private def _lock {
    if (byKey)
      lockByKey(_key)
    else
      lock 
  }
  
  private def _unlock {
    if (byKey)
      unlockByKey(_key)
    else
      unlock     
  }
  
  def get() : Object = {
    ensureZK
    var userLock = true
    // check if the user already gets the lock
    if (!hasLock()) {
      _lock

      val stat = new Stat()
      var rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      var metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      var readsLen = metaData.getReadsCount()
      
      if (readsLen == 0) {
        // no value has been set
        _unlock
        return null
      }
  
      /* read phase 1 */
      var reads = metaData.getReadsList()
      val mostRecentVersion = metaData.getReadsList().get(readsLen - 1)
      // retrieve hdfs path for read and increase read count of this version
      val hdfsPath = this.conf.hdfs_address + this.conf.node_path + keyPath
      var cnt = mostRecentVersion.getNumReaders()
      // rebuild the metadata
      var builder = SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(metaData.getNextVersion())
      for(i <- 0 to readsLen - 2) {
        builder.addReads(reads.get(i))
      }
      var modifiedVersion = SharedInodeProto.SharedInode.VersionNode.newBuilder()
      modifiedVersion.setVersion(mostRecentVersion.getVersion)
      modifiedVersion.setNumReaders(cnt + 1)
      builder.addReads(modifiedVersion)
      zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)
      // release lock if the user does not have the lock

      _unlock

      // read from hdfs
      val fsuri = URI.create(this.conf.hdfs_address)
      val conf = new Configuration()
      val fs = FileSystem.get(fsuri, conf)
      val fsis = fs.open(new Path(URI.create(hdfsPath + "/" + mostRecentVersion.getVersion)))
      val ois = new ObjectInputStream(fsis)
      val res = ois.readObject()
      fsis.close()
      
      /* read phase 2 */

      _lock

      rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      reads = metaData.getReadsList()
      readsLen = metaData.getReadsCount()
      var pos = -1
      for(i <- 0 to readsLen - 1) {
        if (reads.get(i).getVersion() == mostRecentVersion.getVersion())
          pos = i
      }
      if (pos < 0) {
        _unlock
        throw new RuntimeException(this.conf.node_path + keyPath + " version " + 
            mostRecentVersion.getVersion() + " is lost during read");
      }
      builder = SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(metaData.getNextVersion())
      for(i <- 0 to readsLen - 1) {
        var remain = reads.get(i).getNumReaders()
        if (i == pos) remain -= 1
        if (remain == 0 && i < readsLen - 1) {
          // delete the versions which will never be read
          fs.delete(new Path(URI.create(hdfsPath + "/" + reads.get(i).getVersion())), true)
        } else {
          modifiedVersion = SharedInodeProto.SharedInode.VersionNode.newBuilder()
          modifiedVersion.setVersion(reads.get(i).getVersion)
          modifiedVersion.setNumReaders(remain)
          builder.addReads(modifiedVersion)
        }
      }
      zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)

      _unlock
      
  //    fs.close()
  
      return res
    } else {
      val stat = new Stat()
      var rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      var metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      var readsLen = metaData.getReadsCount()
      
      if (readsLen == 0) {
        // no value has been set
        return null
      }
  
      /* read phase 1 */
      var reads = metaData.getReadsList()
      val mostRecentVersion = metaData.getReadsList().get(readsLen - 1)
      // retrieve hdfs path for read
      val hdfsPath = this.conf.hdfs_address + this.conf.node_path + keyPath      
      // read from hdfs
      val fsuri = URI.create(this.conf.hdfs_address)
      val conf = new Configuration()
      val fs = FileSystem.get(fsuri, conf)
      val fsis = fs.open(new Path(URI.create(hdfsPath + "/" + mostRecentVersion.getVersion)))
      val ois = new ObjectInputStream(fsis)
      val res = ois.readObject()
      fsis.close()
      
      /* read phase 2 */
      val builder = SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(metaData.getNextVersion())
      for(i <- 0 to readsLen - 1) {
        var remain = reads.get(i).getNumReaders()
        if (remain == 0 && i < readsLen - 1) {
          // delete the versions which will never be read
          fs.delete(new Path(URI.create(hdfsPath + "/" + reads.get(i).getVersion())), true)
        } else {
          val modifiedVersion = SharedInodeProto.SharedInode.VersionNode.newBuilder()
          modifiedVersion.setVersion(reads.get(i).getVersion)
          modifiedVersion.setNumReaders(remain)
          builder.addReads(modifiedVersion)
        }
      }
      zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)
      
  //    fs.close()
  
      return res
    }
  }
  
  def getByKey(key: String) = {
    _key = key
    keyPath = "/dict/" + stringToHex(key)
    byKey = true
    val result = get()
    _key = "default"
    keyPath = "/default"
    byKey = false
    result
  }
  
  def set(newVal: Any) {
    ensureZK
    // check if the user already gets the lock
    if (!hasLock()) {
      _lock

      /* write phase 1 */
      val stat = new Stat()
      var rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      var metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      var reads = metaData.getReadsList()
      var readsLen = metaData.getReadsCount()
      val version = metaData.getNextVersion()
      var builder = SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(version + 1)
      for(i <- 0 to readsLen - 1) {
        builder.addReads(reads.get(i))
      }
      zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)

      _unlock

      // write data to hdfs
      val fsuri = URI.create(this.conf.hdfs_address)
      val conf = new Configuration()
      val fs = FileSystem.get(fsuri, conf)
      val keyuri = URI.create(this.conf.hdfs_address + this.conf.node_path + keyPath + "/" + version)
      val os = fs.create(new Path(keyuri))
      fs.setPermission(new Path(keyuri), new FsPermission("777"))
      val out = new ObjectOutputStream(os)
      out.writeObject(newVal)
      out.close()
      /* write phase 2 */
      _lock

      rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      reads = metaData.getReadsList()
      readsLen = metaData.getReadsCount()
      if (metaData.getNextVersion() == version + 1) {
        builder = SharedInodeProto.SharedInode.newBuilder()
        builder.setNextVersion(version + 1)
        for(i <- 0 to readsLen - 1) {
          if (reads.get(i).getNumReaders() == 0) {
            // delete the versions which will never be read
            fs.delete(new Path(URI.create(this.conf.hdfs_address + this.conf.node_path + keyPath + "/" + reads.get(i).getVersion())), true)
          } else {
            builder.addReads(reads.get(i))
          }
        }
        val newVersion = SharedInodeProto.SharedInode.VersionNode.newBuilder()
        newVersion.setVersion(version)
        newVersion.setNumReaders(0)
        builder.addReads(newVersion)
        zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)
      } else {
        // delete the file just written because it will never be read
        fs.delete(new Path(URI.create(this.conf.hdfs_address + this.conf.node_path + 
            keyPath + "/" + version)), true)
      }
  //    fs.close()

      _unlock

    } else {
      /* write phase 1 */
      val stat = new Stat()
      var rawData = zk.getData(this.conf.node_path + keyPath, false, stat)
      var metaData = SharedInodeProto.SharedInode.parseFrom(new ByteArrayInputStream(rawData))
      var reads = metaData.getReadsList()
      var readsLen = metaData.getReadsCount()
      val version = metaData.getNextVersion()

      // write data to hdfs
      val fsuri = URI.create(this.conf.hdfs_address)
      val conf = new Configuration()
      val fs = FileSystem.get(fsuri, conf)
      val keyuri = URI.create(this.conf.hdfs_address + this.conf.node_path + keyPath + "/" + version)
      val os = fs.create(new Path(keyuri))
      fs.setPermission(new Path(keyuri), new FsPermission("777"))
      val out = new ObjectOutputStream(os)
      out.writeObject(newVal)
      out.close()

      /* write phase 2 */
      val builder = SharedInodeProto.SharedInode.newBuilder()
      builder.setNextVersion(version + 1)
      for(i <- 0 to readsLen - 1) {
        if (reads.get(i).getNumReaders() == 0) {
          // delete the versions which will never be read
          fs.delete(new Path(URI.create(this.conf.hdfs_address + this.conf.node_path + keyPath + "/" + reads.get(i).getVersion())), true)
        } else {
          builder.addReads(reads.get(i))
        }
      }
      val newVersion = SharedInodeProto.SharedInode.VersionNode.newBuilder()
      newVersion.setVersion(version)
      newVersion.setNumReaders(0)
      builder.addReads(newVersion)
      zk.setData(this.conf.node_path + keyPath, builder.build().toByteArray(), -1)
//    fs.close()
    }
  }
  
  def setByKey(key: String, newVal: Any) {
    _key = key
    keyPath = "/dict/" + stringToHex(key)
    byKey = true
    set(newVal)
    _key = "default"
    keyPath = "/default"
    byKey = false
  }
  
  def destroy {
    if (zk != null) {
      zk.close()
    }
  }
}