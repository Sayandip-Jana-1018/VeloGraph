package velograph.ml

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/** Roadmap P4.2: the pieces of `gold/ml_features`, as pure DataFrame functions (ARCHITECTURE §8.3). */
object FeatureTransforms {

  /** Regime classes (ARCHITECTURE §8.1). */
  val Draining = 0
  val Stable   = 1
  val Filling  = 2

  /**
   * Hour-of-week profiles: each station's mean and spread of departures and arrivals for every (weekday,
   * hour) over the window. The mean is also the seasonal-naive forecast, the baseline every model must beat.
   * Holidays are left out: one Friday off would drag a whole weekday's profile.
   */
  def hourOfWeek(panel: DataFrame, window: DateWindow): DataFrame =
    panel
      .where(window.covers() && !col("is_holiday"))
      .groupBy("station_id", "dow", "hour")
      .agg(
        avg("departures").as("prof_dep_mean"),
        avg("arrivals").as("prof_arr_mean"),
        stddev_pop("departures").as("prof_dep_std"),
        stddev_pop("arrivals").as("prof_arr_std")
      )

  /** The network-wide mean profile per (weekday, hour), for stations that opened after the window. */
  def fallbackProfile(profiles: DataFrame): DataFrame =
    profiles
      .groupBy("dow", "hour")
      .agg(
        avg("prof_dep_mean").as("fb_dep_mean"),
        avg("prof_arr_mean").as("fb_arr_mean"),
        avg("prof_dep_std").as("fb_dep_std"),
        avg("prof_arr_std").as("fb_arr_std")
      )

  /**
   * Joins the profile (or the fallback where a station has none) onto rows keyed by station, dow and hour.
   */
  def withProfiles(rows: DataFrame, profiles: DataFrame): DataFrame =
    rows
      .join(profiles, Seq("station_id", "dow", "hour"), "left")
      .join(broadcast(fallbackProfile(profiles)), Seq("dow", "hour"), "left")
      .withColumn("has_profile", col("prof_dep_mean").isNotNull)
      .withColumn("prof_dep_mean", coalesce(col("prof_dep_mean"), col("fb_dep_mean")))
      .withColumn("prof_arr_mean", coalesce(col("prof_arr_mean"), col("fb_arr_mean")))
      .withColumn("prof_dep_std", coalesce(col("prof_dep_std"), col("fb_dep_std")))
      .withColumn("prof_arr_std", coalesce(col("prof_arr_std"), col("fb_arr_std")))
      .drop("fb_dep_mean", "fb_arr_mean", "fb_dep_std", "fb_arr_std")

  /** Hour of day on a circle, so 23:00 and 00:00 are neighbours for a linear model. */
  def hourOnCircle(hour: Column): (Column, Column) = {
    val angle = hour * lit(2 * math.Pi / 24)
    (sin(angle), cos(angle))
  }

  /**
   * The regime of a station-hour: draining when it loses more than `tau` of its capacity in net flow, filling
   * when it gains more, stable otherwise.
   */
  def regime(netFlow: Column, capacity: Column, tau: Double): Column = {
    val relative = netFlow / capacity
    when(relative < -tau, Draining).when(relative > tau, Filling).otherwise(Stable)
  }

  /**
   * Community ids are arbitrary labels, so each window numbers its communities its own way. This maps every
   * community of `other` to the `reference` community holding most of its stations when that is at least half
   * of them, and otherwise to a fresh id above the reference's, so a community id means the same district in
   * the fit and the test rows. Community 0 ("no strong ties") stays 0. Driver-side: one entry per station.
   */
  def alignCommunities(reference: Map[String, Int], other: Map[String, Int]): Map[Int, Int] = {
    val fresh = Iterator.from(reference.values.foldLeft(0)(math.max) + 1)
    other
      .filter(_._2 > 0)
      .groupBy(_._2)
      .toSeq
      .sortBy { case (id, members) => (-members.size, id) }
      .map { case (id, members) =>
        val votes = members.keys.toSeq.flatMap(reference.get).filter(_ > 0).groupBy(identity).map {
          case (c, v) => c -> v.size
        }
        val best = votes.toSeq.sortBy { case (c, n) => (-n, c) }.headOption
        id -> best.collect { case (c, n) if 2 * n >= members.size => c }.getOrElse(fresh.next())
      }
      .toMap + (0 -> 0)
  }
}
