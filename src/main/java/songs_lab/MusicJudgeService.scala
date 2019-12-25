package songs_lab

/**
  * @author Evgeny Borisov
  */
class MusicJudgeService(topWordsService: TopWordsService) {

  def topX(artist:String,x:Int): Unit ={
    val rdd = SparkHolder.sc.textFile(s"data/songs/$artist/*")
    val topWords = topWordsService.topX(rdd,x)
    println(topWords)
  }

}
