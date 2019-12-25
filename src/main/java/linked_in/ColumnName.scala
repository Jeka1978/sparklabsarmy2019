package linked_in

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions}

/**
  * @author Evgeny Borisov
  */
object ColumnName {
  val SALARY = "salary"
  val NAME = "name"

  val AGE: String = "age"

  val KEYWORDS: String = "keywords"

  val AMOUNT = "amount"

  val KEYWORD: String = "keyword"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("linked in").master("local[*]").getOrCreate()
    val dataFrame = spark.read.json("data/linkedIn/profiles.json")
    dataFrame.printSchema()
    dataFrame.schema.fields.foreach(f=> println(f.dataType))


    val salaryDf = dataFrame.withColumn(SALARY, col(AGE)*size(col(KEYWORDS))*10)
    salaryDf.persist()
    salaryDf.show()

    println("********** convert to dataset ******")
    import spark.implicits._

    val employees: Dataset[Employee] = salaryDf.as
    val employeeList: Array[Employee] = employees.collect()
    employeeList.foreach(println(_))
    println("**********")


    val mostPopular: String = salaryDf.withColumn(KEYWORD, explode(col(KEYWORDS))).select(KEYWORD)
      .groupBy(col(KEYWORD)).agg(count(col(KEYWORD)).as(AMOUNT))
      .sort(col(AMOUNT).desc).first().getAs[String](KEYWORD)


    salaryDf
      .filter(col(SALARY)<=1200)
      .filter(array_contains(col(KEYWORDS),mostPopular)).show()








  }
}




