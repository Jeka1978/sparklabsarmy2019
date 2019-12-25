package linked_in

import org.apache.spark.sql.SparkSession
import songs_lab.SparkHolder

/**
  * @author Evgeny Borisov
  */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("linked in").master("local[*]").getOrCreate()
    val dataFrame = spark.read.json("data/linkedIn/profiles.json")
    dataFrame.show()
  }
}
