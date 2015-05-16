package edu.ucla.cs249

class SerObj(name: String, id: Int) extends Serializable {
  def getname = name
  def getid = id
}

object VarTest {
  def main(args: Array[String]) {
    println("----- VarTest -----")
    var myi = new SparkConcurrentVariable
//    var obj = new SerObj("name", 123)
//    myi.set(obj)
    
    var obj = myi.get()
    obj match {
      case serobj: SerObj => 
        println(serobj.getname)
        println(serobj.getid)
    } 
  }
}