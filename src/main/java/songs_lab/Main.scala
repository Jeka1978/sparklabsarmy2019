package songs_lab

import org.apache.spark.sql.functions

/**
  * @author Evgeny Borisov
  */
object Main {
  def main(args: Array[String]): Unit = {


    val service = new MusicJudgeService(new TopWordsService)
    val topX = service.topX("britney",3)
    println(topX)
  }
}
