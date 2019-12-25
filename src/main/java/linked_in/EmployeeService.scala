package linked_in

import linked_in.ColumnName._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import collection.JavaConverters._

/**
  * @author Evgeny Borisov
  */
@Component
class EmployeeService {


  var currencyServices: List[CurrencyService] = Nil

  @Autowired
  def setCurrencyServices(list:java.util.List[CurrencyService]):Unit={
    currencyServices=list.asScala.toList
  }

  def processEmployees(dataFrame: DataFrame): DataFrame = {
    var salaryDf = dataFrame.withColumn(SALARY, col(AGE) * size(col(KEYWORDS)) * 10)
    salaryDf.persist()
    currencyServices.foreach(currencyService => salaryDf = currencyService.withCurrencyColumn(salaryDf))
    salaryDf
  }


}
