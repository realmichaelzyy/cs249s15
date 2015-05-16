package edu.ucla.cs249

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import org.apache.hadoop.fs.permission.FsPermission

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
      fs.mkdirs(new Path(uri), perm)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}