package songs_lab

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Evgeny Borisov
  */
object SparkHolder {

  val sc:SparkContext = new SparkContext(new SparkConf().setAppName("songs").setMaster("local[*]"))
}
