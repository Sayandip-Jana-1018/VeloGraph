package velograph.ml

import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

import velograph.gold.PeakHours

/**
 * A station's typical day as four 24-hour curves, in shares of its whole profile: departures and arrivals on
 * a working day, and on a day off. A KMeans centre reads back the same way.
 */
final case class DayShape(
    workDep: Vector[Double],
    workArr: Vector[Double],
    offDep: Vector[Double],
    offArr: Vector[Double]
) {
  def work: Double = workDep.sum + workArr.sum
  def off: Double  = offDep.sum + offArr.sum

  /** Shares of departures and arrivals in the working-day hours [from, until). */
  def departuresIn(from: Int, until: Int): Double = workDep.slice(from, until).sum
  def arrivalsIn(from: Int, until: Int): Double   = workArr.slice(from, until).sum
}

object DayShape {

  /**
   * Reads a 96-slot profile vector: [work departures 0-23 | work arrivals | off departures | off arrivals].
   */
  def apply(slots: Array[Double]): DayShape = {
    require(slots.length == ArchetypeTransforms.Slots, s"a profile has 96 slots, got ${slots.length}")
    def block(b: Int) = slots.slice(b * 24, (b + 1) * 24).toVector
    DayShape(block(0), block(1), block(2), block(3))
  }
}

/** Roadmap P4.1: hourly share profiles of stations, and the names their clusters earn. */
object ArchetypeTransforms {

  val Slots = 96

  /** Holidays ride like weekends, so both count as days off. */
  val DayOff: Column = col("is_weekend") || col("is_holiday")

  /**
   * One row per station with any trips in the window. For each hour of a working day and of a day off it
   * takes the mean departures and arrivals over the window's days, then scales the 96 values to sum to 1
   * (`hourly`). That describes the shape of a station's day, not its size, so a quiet and a busy station with
   * the same rhythm look alike. `shares` is the same profile summed into `binHours`-hour bins, the vector
   * KMeans sees: with eight samples per weekday hour, single hours are noisy. `trips` (departures + arrivals
   * in the window) lets thin profiles stay out of the fit.
   */
  def profiles(panel: DataFrame, window: DateWindow, binHours: Int): DataFrame = {
    require(24 % binHours == 0, s"bins of $binHours hours do not tile a day")
    val rows  = panel.where(window.covers())
    val block = when(DayOff, 2).otherwise(0) // working days fill slots 0-47, days off 48-95
    val slots = rows
      .groupBy(col("station_id"), block.as("block"), col("hour"))
      .agg(avg("departures").as("dep"), avg("arrivals").as("arr"))
      .select(
        col("station_id"),
        explode(
          array(
            struct((col("block") * 24 + col("hour")).as("slot"), col("dep").as("value")),
            struct(((col("block") + 1) * 24 + col("hour")).as("slot"), col("arr").as("value"))
          )
        ).as("s")
      )
    val raw = slots
      .groupBy("station_id")
      .agg(map_from_entries(collect_list(struct(col("s.slot"), col("s.value")))).as("by_slot"))
      .select(
        col("station_id"),
        // a station open only on working days in the window has no day-off slots: those are zero
        transform(
          sequence(lit(0), lit(Slots - 1)),
          i => coalesce(element_at(col("by_slot"), i), lit(0.0))
        ).as("raw")
      )
      .withColumn("total", aggregate(col("raw"), lit(0.0), (sum, x) => sum + x))
    val trips = rows.groupBy("station_id").agg(sum(col("departures") + col("arrivals")).as("trips"))
    val bins  = sequence(lit(0), lit(Slots / binHours - 1))
    raw
      .where(col("total") > 0)
      .select(col("station_id"), transform(col("raw"), x => x / col("total")).as("hourly"))
      .withColumn(
        "shares", // bins never straddle two curves, because each curve is 24 slots and binHours divides 24
        array_to_vector(
          transform(
            bins,
            b => aggregate(slice(col("hourly"), b * binHours + 1, lit(binHours)), lit(0.0), (s, x) => s + x)
          )
        )
      )
      .join(trips, "station_id")
  }

  /**
   * Names a centre by its shape, with rules stated in config (ARCHITECTURE §8):
   *   - "Leisure" when a day off is at least `leisureRatio` as busy as a working day;
   *   - "Morning origin" when working-day AM-peak departures are `commuteRatio` times the arrivals (riders
   *     leave in the morning and come back in the evening, the rhythm of a home end), "Strong" from
   *     `strongRatio`;
   *   - "Morning destination" for the reverse (the rhythm of a workplace end);
   *   - "All-day mix" otherwise.
   */
  def name(shape: DayShape, peaks: PeakHours, c: ArchetypeConfig): String = {
    val leaving               = shape.departuresIn(peaks.amStart, peaks.amEnd)
    val arriving              = shape.arrivalsIn(peaks.amStart, peaks.amEnd)
    def strong(ratio: Double) = if (ratio >= c.strongRatio) "Strong morning" else "Morning"
    if (shape.work == 0.0 || shape.off / shape.work >= c.leisureRatio) "Leisure"
    else if (leaving >= c.commuteRatio * arriving) s"${strong(leaving / arriving)} origin"
    else if (arriving >= c.commuteRatio * leaving) s"${strong(arriving / leaving)} destination"
    else "All-day mix"
  }

  /**
   * Makes names unique in the given order (largest cluster first): "Morning origin", "Morning origin 2", ...
   */
  def distinct(names: Seq[String]): Seq[String] =
    names.zipWithIndex.map { case (n, i) =>
      val earlier = names.take(i).count(_ == n)
      if (earlier == 0) n else s"$n ${earlier + 1}"
    }
}
