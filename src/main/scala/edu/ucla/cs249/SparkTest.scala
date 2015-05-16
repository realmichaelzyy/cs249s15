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

/** Computes an approximation to pi */
object SparkTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100L * slices, Int.MaxValue).toInt // avoid overflow
    
    var serobj = new SerObj("qqq", 5)
    val count = spark.parallelize(1 until 100).map { i =>
//      val zk = new ZooKeeper("54.88.56.9:2181", 5000, null)
//      zk.close()
//      var builder = SharedInodeProto.SharedInode.newBuilder()
      
      serobj.getid
    }.reduce(_ + _)
    println("count: " + count)
    spark.stop()
  }
}
