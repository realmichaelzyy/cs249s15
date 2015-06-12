package edu.ucla.cs249

import org.apache.spark._
import org.apache.zookeeper._
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import java.io.ObjectOutputStream


object ConcurrentWriteTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CS 249 Concurrent Write Test")

    val spark = new SparkContext(conf)
    val hdfsAddr = System.getenv("HDFS_ADDRESS")
    
    var beforeParallelize = Calendar.getInstance.getTimeInMillis
    val count = spark.parallelize(0 until 300).map { i =>
      var obj_ = new BigObj()
      for (i <- 0 until 10000000) {
        obj_.arr.+=(1)
      }
      
      val fsuri = URI.create(hdfsAddr)
      val fsconf = new Configuration()
      val fs = FileSystem.get(fsuri, fsconf)
      val keyuri = URI.create(hdfsAddr + "/test/" + i)
      val os = fs.create(new Path(keyuri))
      fs.setPermission(new Path(keyuri), new FsPermission("777"))
      val out = new ObjectOutputStream(os)
      out.writeObject(obj_)
      out.close()
      fs.delete(new Path(keyuri), true)
      1
    }.count
    
    println("\n-------------\ncount: " + count + "\n---------------")
    var afterParallelize = Calendar.getInstance.getTimeInMillis
    println("\n-------------\ntime lapse: " + (afterParallelize-beforeParallelize) + "\n--------------\n")

    spark.stop()
  }
}
