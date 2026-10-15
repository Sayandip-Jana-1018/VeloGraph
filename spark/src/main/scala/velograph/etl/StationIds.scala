package velograph.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat, lit, when}

/**
 * Canonical Citi Bike station ids. Ids always carry two decimals (`5980.10`), but about 1 % of trip records
 * and 3 GBFS entries dropped the trailing zero (`5980.1`). Left alone, one station would become two graph
 * vertices and miss its GBFS match, so a single decimal digit gets its zero back. Every place that joins on
 * station ids (silver trips, the GBFS registry, the RDD demo, the live feed) goes through this one rule.
 */
object StationIds {

  private val OneDecimal = "^\\d+\\.\\d$"

  def normalize(id: Column): Column = when(id.rlike(OneDecimal), concat(id, lit("0"))).otherwise(id)

  def normalize(id: String): String = if (id != null && id.matches(OneDecimal)) id + "0" else id
}
