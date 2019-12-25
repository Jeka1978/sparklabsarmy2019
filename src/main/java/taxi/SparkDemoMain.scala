package taxi

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Evgeny Borisov
  */
object SparkDemoMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd = sc.textFile("data/taxi_orders.txt")
    val tripRdd = rdd.map(line => {
      val strings = line.split(" ")
      Trip(strings(0).toInt, strings(1), strings(2).toInt)
    })
    tripRdd.persist()

    val totalTripsToBoston = tripRdd.filter(_.city.toLowerCase == "boston").filter(_.km > 10).count()
    tripRdd.collect().foreach(println(_))

    val totalKmBoston = tripRdd.filter(_.city.toLowerCase == "boston")
      .map(_.km).sum()
    println(s"total trips to boston $totalTripsToBoston")
    println(totalKmBoston)

    val top3Array = tripRdd.map(trip => Tuple2(trip.id, trip.km))
      .reduceByKey(_ + _)

      .sortBy(_._2, ascending = false).take(3)

    val top3Rdd = sc.parallelize(top3Array)

    val driversDataRdd = sc.textFile("data/drivers.txt")
      .map(line => {
        val arr = line.split(", ")
        Tuple2(arr(0).toInt, arr(1))
      })

    top3Rdd.join(driversDataRdd).collect().foreach(println(_))


    println("******************************")


    var smallTrips = sc.longAccumulator
    tripRdd.foreach(trip => {
      if (trip.km < 5) smallTrips.add(1)
    })

    println(smallTrips.value)

    Thread.sleep(1000000)

  }
}






