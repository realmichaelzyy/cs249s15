package edu.ucla.cs249

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

object ScalaHdfsTest {
  def main(args: Array[String]) {
    println("----- Scala HDFS Test -----")
    try {
      val conf = new Configuration ();
      val fs = FileSystem.get(conf);
      val uri = URI.create ("hdfs://54.88.56.9:8020/vardev");
      fs.mkdirs(new Path(uri))
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}