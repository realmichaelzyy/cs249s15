package edu.ucla.cs249

import org.apache.zookeeper._
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream

class SharedObject[T <: Serializable] {
    private var value = null;
    def get() = {
      value.toString()
    }
    
    def set(newVal: T) {
      val b = new ByteArrayOutputStream()
      val o = new ObjectOutputStream(b)
      o.writeObject(newVal)
      val byteArr = b.toByteArray()
    }
}