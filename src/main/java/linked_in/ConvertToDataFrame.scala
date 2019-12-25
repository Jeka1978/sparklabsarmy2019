package linked_in

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import songs_lab.SparkHolder

/**
  * @author Evgeny Borisov
  */
object ConvertToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("stam").master("local[*]").getOrCreate()
    val rdd: RDD[Stam] = spark.sparkContext.parallelize(List(Stam("abc", "zyz"), Stam("123", "456")))

    import spark.implicits._
    rdd.toDF().show()

  }
}
