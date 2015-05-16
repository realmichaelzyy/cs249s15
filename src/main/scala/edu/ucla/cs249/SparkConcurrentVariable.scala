package edu.ucla.cs249

import org.apache.zookeeper._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import org.apache.zookeeper.data.Stat
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream
import java.io.Serializable

class SparkConcurrentVariable {

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