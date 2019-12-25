package linked_in

import linked_in.ColumnName._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.springframework.stereotype.Component

/**
  * @author Evgeny Borisov
  */

@Component
class NisCurrencyService extends CurrencyService {
  override def withCurrencyColumn(dataFrame: DataFrame): DataFrame = {
    val myUdf = udf { salary: Int => {
      salary * CurrencyHolder.currencies("nis")
    }
    }
    dataFrame.withColumn("salary in NIS", myUdf(col(SALARY)))
  }
}










