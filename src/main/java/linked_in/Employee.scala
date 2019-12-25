package linked_in

import org.apache.spark.sql.Encoder

/**
  * @author Evgeny Borisov
  */
case class Employee(name:String,age:BigInt,salary:BigInt,keywords: List[String]) {

}
