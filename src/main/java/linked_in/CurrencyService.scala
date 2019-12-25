package linked_in

import org.apache.spark.sql.DataFrame

/**
  * @author Evgeny Borisov
  */
trait CurrencyService {
  def withCurrencyColumn(dataFrame: DataFrame):DataFrame

}
