/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.ucla.cs249

import scala.math.random
import org.apache.spark._
import org.apache.zookeeper._
import scala.collection.mutable.ArrayBuffer
import java.util.Calendar


class BigObj() extends Serializable {
  var arr = new ArrayBuffer[Int]
}

object WriteTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CS 249 Write Test")

    val spark = new SparkContext(conf)

    val svconf = new SharedVariableConfig(System.getenv("HDFS_ADDRESS"), System.getenv("ZK_CONNECT_STRING"))
    val shared = new SharedVariable(svconf)
    var obj = new BigObj()
    shared.set(obj)
    
    var beforeParallelize = Calendar.getInstance.getTimeInMillis
    val count = spark.parallelize(0 until 10000).map { i =>
      val shared_ = new SharedVariable(svconf)
      var obj_ = new BigObj()
      for (i <- 0 until 10000000) {
        obj_.arr.+=(1)
      }
      
      //shared_.lock
      shared_.set(obj_)
      //shared_.unlock
      
      shared_.destroy
      1
    }.count
    
    println("\n-------------\ncount: " + count + "\n---------------")
    var afterParallelize = Calendar.getInstance.getTimeInMillis
    println("\n-------------\ntime lapse: " + (afterParallelize-beforeParallelize) + "\n--------------\n")
    
    svconf.destroy
    spark.stop()
  }
}
