package songs_lab

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * @author Evgeny Borisov
  */
class TopWordsService extends Serializable {


  private val stopWords = List("the", "you", "i", "and", "for", "a", "they", "in", "to", "me", "we", "t")
  val stopWordsBroadcasted: Broadcast[List[String]] = SparkHolder.sc.broadcast(stopWords)

  def topX(lines: RDD[String], x: Int): List[String] = {


    lines.map(_.toLowerCase)
      .flatMap("\\w+".r.findAllIn(_))
      .filter(word => !this.stopWordsBroadcasted.value.contains(word))
      .map(Tuple2(_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .filter(_._1 != "table")

      .take(x)
      .map(_._1)
      .toList

  }

}
