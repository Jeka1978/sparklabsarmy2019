package linked_in

import org.apache.spark.sql.SparkSession
import org.springframework.context.annotation.AnnotationConfigApplicationContext

/**
  * @author Evgeny Borisov
  */
object Start {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("linked in").master("local[*]").getOrCreate()
    val dataFrame = spark.read.json("data/linkedIn/profiles.json")

    val context = new AnnotationConfigApplicationContext("linked_in")
    val service = context.getBean(classOf[EmployeeService])



    service.processEmployees(dataFrame).show()
  }
}
