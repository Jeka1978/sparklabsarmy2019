package songs

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}
import songs_lab.TopWordsService

/**
  * @author Evgeny Borisov
  */
class TopWordsServiceTest {

  @Test
  def testTopX():Unit={
    val sc = new SparkContext(new SparkConf().setAppName("songs").setMaster("local[*]"))
    val rdd = sc.parallelize(List("java java java", "scala scala", "python"))
    val wordsService = new TopWordsService()
    val list = wordsService.topX(rdd,2)

    Assert.assertEquals(2,list.size)
    Assert.assertEquals("java", list.head)
    Assert.assertEquals("scala", list(1))



  }
}
