package edu.ucla.cs249

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

object ScalaHdfsTest {
  def main(args: Array[String]) {
    println("----- Scala HDFS Test -----")
    try {
      val fsuri = URI.create ("hdfs://54.88.56.9:8020/")
      val conf = new Configuration ()
      val fs = FileSystem.get(fsuri, conf)
      val perm = new FsPermission("777")
      val uri = URI.create ("hdfs://54.88.56.9:8020/dev/vardev")
      println(perm.toString())
      fs.mkdirs(new Path(uri))
      fs.setPermission(new Path(uri), perm)
      
      val obj = new SerObj("name", 1234)
      val furi = URI.create ("hdfs://54.88.56.9:8020/dev/vardev/test0.setobj")
//      val fsos = fs.create(new Path(furi))
//      val oos = new ObjectOutputStream(fsos)
//      oos.writeObject(obj)
//      fs.setPermission(new Path(furi), perm)
      
      val fsis = fs.open(new Path(furi))
      val ois = new ObjectInputStream(fsis)
      val readobj = ois.readObject()
      readobj match {
        case serobj: SerObj => 
          println(serobj.getname)
          println(serobj.getid)
      }
      
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}