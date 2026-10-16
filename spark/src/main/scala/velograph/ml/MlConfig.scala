package velograph.ml

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import com.typesafe.config.Config
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}

/** An inclusive range of New York dates. */
final case class DateWindow(name: String, from: LocalDate, to: LocalDate) {
  require(!to.isBefore(from), s"window $name ends ($to) before it starts ($from)")

  def days: Long = ChronoUnit.DAYS.between(from, to) + 1

  def contains(date: LocalDate): Boolean = !date.isBefore(from) && !date.isAfter(to)

  /** True for rows whose `date` column falls inside the window. */
  def covers(date: Column = col("date")): Column = date.between(lit(from.toString), lit(to.toString))

  override def toString: String = s"$name $from..$to"
}

/**
 * One leakage-safe split (ARCHITECTURE §8.2): the rows to model, and the trailing window their profile, graph
 * and archetype features are computed from. A profile window that does not end before the rows start would
 * let the features see the very hours they are meant to predict, so it cannot be constructed.
 */
final case class FeatureWindow(split: String, rows: DateWindow, profile: DateWindow) {
  require(
    profile.to.isBefore(rows.from),
    s"leakage: ${profile.name} (${profile.from}..${profile.to}) must end before ${rows.name} starts (${rows.from})"
  )
}

final case class ArchetypeConfig(
    kMin: Int,
    kMax: Int,
    maxIter: Int,
    binHours: Int,
    restarts: Int,
    minClusterShare: Double,
    minTrips: Long,
    commuteRatio: Double,
    strongRatio: Double,
    leisureRatio: Double
) {
  require(2 <= kMin && kMin <= kMax, s"need 2 <= k-min <= k-max, got $kMin..$kMax")
  require(24 % binHours == 0, s"bin-hours must divide a day evenly, got $binHours")
  require(restarts >= 1, s"restarts must be at least 1, got $restarts")
}

/** `velograph.ml` in application.conf. */
final case class MlConfig(
    seed: Long,
    fit: FeatureWindow,
    test: FeatureWindow,
    validation: DateWindow,
    archetypes: ArchetypeConfig,
    regimeQuantile: Double
) {
  require(
    validation.from.isAfter(fit.rows.from) && !validation.to.isAfter(fit.rows.to),
    s"the validation week ($validation) must be the end of the fit rows (${fit.rows})"
  )
  require(test.rows.from.isAfter(fit.rows.to), s"the test rows (${test.rows}) must come after the fit rows")

  /** Both splits, fit first. */
  def splits: Seq[FeatureWindow] = Seq(fit, test)
}

object MlConfig {

  def apply(raw: Config): MlConfig = {
    val c                                = raw.getConfig("velograph.ml")
    def window(name: String): DateWindow =
      DateWindow(
        name,
        LocalDate.parse(c.getString(s"windows.$name.from")),
        LocalDate.parse(c.getString(s"windows.$name.to"))
      )
    val a = c.getConfig("archetypes")
    MlConfig(
      seed = c.getLong("seed"),
      fit = FeatureWindow("fit", window("fit"), window("profile-a")),
      test = FeatureWindow("test", window("test"), window("profile-b")),
      validation = window("validation"),
      archetypes = ArchetypeConfig(
        a.getInt("k-min"),
        a.getInt("k-max"),
        a.getInt("max-iter"),
        a.getInt("bin-hours"),
        a.getInt("restarts"),
        a.getDouble("min-cluster-share"),
        a.getLong("min-trips"),
        a.getDouble("commute-ratio"),
        a.getDouble("strong-ratio"),
        a.getDouble("leisure-ratio")
      ),
      regimeQuantile = c.getDouble("regime.quantile")
    )
  }
}
