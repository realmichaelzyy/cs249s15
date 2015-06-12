Mutable Shared Variables for Spark
=========
Authors: Bochun Zhang & Sheng hu

#What's this
This project is a library to support mutable shared variable for Spark.

###Sample usage (Scala)
```scala
val svconf = new SharedVariableConfig("hdfs://...", "localhost:2181")
val count = spark.parallelize(0 until 100).map { i =>
    val shared = new SharedVariable(svconf)
    var obj = new MySerializableInteger()
    obj.value = 0
    shared.lock
    shared.set(obj)
    shared.unlock
    shared.destroy
    1
}.count
```

#Build

```shell
mvn install
```

#Run the tests

###Set the following environment variables

####ZooKeeper Connect String

> ZK_CONNECT_STRING e.g. "54.88.56.9:2181"

####HDFS address

> HDFS_ADDRESS e.g. "hdfs://54.88.56.9:8020"

###Example command to submit test to Spark

```shell
$SPARK_HOME/bin/spark-submit --class edu.ucla.cs249.SparkTest --jars target/lib/*.jar,target/lib/*.jar target/cs249s15-1.0-SNAPSHOT.jar --master yarn://ip-172-31-53-145.ec2.internal
```




