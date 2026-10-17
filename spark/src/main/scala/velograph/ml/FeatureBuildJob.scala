package velograph.ml

import java.time.LocalDate
import java.time.temporal.ChronoUnit

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import velograph.common.{JobContext, ServingJson, SourceRange, SparkJob}
import velograph.etl.{LakeSchemas, LakeTable, LakeTables}
import velograph.gold.{GoldConfig, PanelSpan, RouteTransforms}
import velograph.graph.{GraphConfig, GraphFeatures, StrongEdgeRule}

/** One entry of the feature dictionary (`serving/ml-features.json`); the labs select features by group. */
final case class FeatureSpec(name: String, group: String, kind: String, description: String)
final case class SplitWindows(split: String, rows: SourceRange, profile: SourceRange)
final case class LeakageCheck(split: String, lastProfileDay: String, firstRowDay: String, passed: Boolean)
final case class SplitStats(
    split: String,
    rows: Long,
    validationRows: Long,
    stations: Long,
    withoutProfile: Long,
    withoutArchetype: Long,
    draining: Long,
    stable: Long,
    filling: Long,
    meanDepartures: Double
)
private[ml] final case class Built(
    split: FeatureWindow,
    rows: DataFrame,
    graph: DataFrame,
    check: LeakageCheck
)
final case class FeatureReport(
    windows: Seq[SplitWindows],
    validation: SourceRange,
    tau: Double,
    tauQuantile: Double,
    strongMinTrips: Long,
    medianCapacity: Int,
    communitiesMatched: Int,
    communitiesNew: Int,
    leakage: Seq[LeakageCheck],
    splits: Seq[SplitStats],
    features: Seq[FeatureSpec],
    targets: Seq[FeatureSpec]
)

/**
 * Roadmap P4.2: `gold/ml_features`. Every station-hour of the fit (August) and test (September) splits, with
 * calendar and weather from the hour itself, and station, graph, archetype and hour-of-week profile features
 * from the split's trailing profile window only (ARCHITECTURE §8.2). Two checks guard against leakage:
 * MlConfig refuses a profile window that does not end before its rows start, and this job checks the same on
 * the data it actually read. The regime threshold tau is chosen on the fit rows and applied unchanged to the
 * test rows.
 */
object FeatureBuildJob extends SparkJob {

  val jobName = "feature-build"

  val Dictionary: Seq[FeatureSpec] = Seq(
    FeatureSpec("hour", "calendar", "numeric", "hour of day, 0-23 (trees split it directly)"),
    FeatureSpec("hour_sin", "calendar", "numeric", "hour of day on a circle, sine"),
    FeatureSpec("hour_cos", "calendar", "numeric", "hour of day on a circle, cosine"),
    FeatureSpec("dow", "calendar", "categorical", "ISO weekday, 1 = Monday"),
    FeatureSpec("is_weekend", "calendar", "flag", "Saturday or Sunday"),
    FeatureSpec("is_holiday", "calendar", "flag", "US federal holiday, observed date"),
    FeatureSpec("temp_c", "weather", "numeric", "air temperature, °C"),
    FeatureSpec("apparent_temp_c", "weather", "numeric", "feels-like temperature, °C"),
    FeatureSpec("precip_mm", "weather", "numeric", "precipitation in the hour, mm"),
    FeatureSpec("wind_kmh", "weather", "numeric", "wind speed, km/h"),
    FeatureSpec("humidity_pct", "weather", "numeric", "relative humidity, %"),
    FeatureSpec("cloud_pct", "weather", "numeric", "cloud cover, %"),
    FeatureSpec("is_raining", "weather", "flag", "measurable rain in the hour"),
    FeatureSpec("capacity", "station", "numeric", "docks (median where GBFS lists none)"),
    FeatureSpec("lat", "station", "numeric", "latitude"),
    FeatureSpec("lon", "station", "numeric", "longitude"),
    FeatureSpec(
      "archetype",
      "station",
      "categorical",
      "KMeans archetype of the profile window (-1: no profile)"
    ),
    FeatureSpec("pagerank_w", "graph", "numeric", "weighted PageRank of the window's route graph"),
    FeatureSpec("pr_am", "graph", "numeric", "PageRank on working-day morning-peak trips"),
    FeatureSpec("pr_pm", "graph", "numeric", "PageRank on working-day evening-peak trips"),
    FeatureSpec("pr_shift", "graph", "numeric", "(pr_am - pr_pm) / (pr_am + pr_pm)"),
    FeatureSpec("in_strength", "graph", "numeric", "trips arriving in the window"),
    FeatureSpec("out_strength", "graph", "numeric", "trips leaving in the window"),
    FeatureSpec("imbalance_ratio", "graph", "numeric", "(in - out) / (in + out) strength"),
    FeatureSpec(
      "clustering_coef",
      "graph",
      "numeric",
      "local clustering coefficient of the strong-tie graph"
    ),
    FeatureSpec(
      "community",
      "graph",
      "categorical",
      "label-propagation district, aligned across windows (0: none)"
    ),
    FeatureSpec(
      "prof_dep_mean",
      "profile",
      "numeric",
      "mean departures in this weekday-hour over the window"
    ),
    FeatureSpec("prof_arr_mean", "profile", "numeric", "mean arrivals in this weekday-hour over the window"),
    FeatureSpec("prof_dep_std", "profile", "numeric", "spread of departures in this weekday-hour"),
    FeatureSpec("prof_arr_std", "profile", "numeric", "spread of arrivals in this weekday-hour"),
    FeatureSpec("has_profile", "profile", "flag", "false: the network-wide profile stands in")
  )

  val Targets: Seq[FeatureSpec] = Seq(
    FeatureSpec("departures", "target", "count", "trips leaving the station in the hour (regression)"),
    FeatureSpec("arrivals", "target", "count", "trips arriving in the hour (regression)"),
    FeatureSpec("net_flow", "target", "count", "arrivals - departures"),
    FeatureSpec(
      "regime",
      "target",
      "class",
      "0 draining, 1 stable, 2 filling, against tau x capacity (classification)"
    )
  )

  private def round(x: Double, digits: Int): Double = math.rint(x * math.pow(10, digits)) /
    math.pow(10, digits)

  /** The median dock count of trip stations: the capacity of the few that GBFS no longer lists. */
  def medianCapacity(stations: DataFrame): Int =
    stations
      .where(col("in_trips") && col("capacity").isNotNull)
      .stat
      .approxQuantile("capacity", Array(0.5), 0.0)
      .head
      .toInt

  /** Capacity (the median where GBFS lists none, flagged), latitude and longitude. */
  def stationAttributes(stations: DataFrame, medianCapacity: Int): DataFrame =
    stations.select(
      col("station_id"),
      coalesce(col("capacity"), lit(medianCapacity)).as("capacity"),
      col("capacity").isNotNull.as("capacity_known"),
      col("lat"),
      col("lon")
    )

  /**
   * Strong ties are "at least N trips"; N is set for the four-month graph, so it is scaled to each window.
   */
  def strongRule(graph: GraphConfig, span: PanelSpan, w: DateWindow): StrongEdgeRule = {
    val panelDays = ChronoUnit.DAYS.between(LocalDate.parse(span.from), LocalDate.parse(span.to)) + 1
    StrongEdgeRule(
      math.max(1L, math.round(graph.strong.minTrips.toDouble * w.days / panelDays)),
      graph.strong.topK
    )
  }

  def run(ctx: JobContext, args: Seq[String]): Map[String, Any] = {
    val spark = ctx.spark
    import spark.implicits._
    val lake     = new LakeTables(spark, ctx.paths)
    val ml       = MlConfig(ctx.cfg.raw)
    val graphCfg = GraphConfig(ctx.cfg.raw)
    val span     = GoldConfig.span(ctx.cfg.raw)
    val peaks    = GoldConfig.peaks(ctx.cfg.raw)

    val panel      = lake.read(LakeTable.StationHour)
    val trips      = lake.read(LakeTable.SilverTrips)
    val stations   = lake.read(LakeTable.Stations)
    val calendar   = lake.read(LakeTable.Calendar)
    val archetypes = lake.read(LakeTable.Archetypes)

    val median                    = medianCapacity(stations)
    val stationAttrs              = stationAttributes(stations.where(col("in_trips")), median)
    def strongRule(w: DateWindow) = FeatureBuildJob.strongRule(graphCfg, span, w)

    def build(split: FeatureWindow): Built = {
      val w = split.profile
      log.info(s"${split.split}: rows ${split.rows}, features from ${w}")
      val lastProfileDay =
        panel.where(w.covers()).agg(max("date")).head().getDate(0).toLocalDate // bounded: 1 row
      val firstRowDay = panel.where(split.rows.covers()).agg(min("date")).head().getDate(0).toLocalDate
      val check       =
        LeakageCheck(
          split.split,
          lastProfileDay.toString,
          firstRowDay.toString,
          lastProfileDay.isBefore(firstRowDay)
        )
      require(
        check.passed,
        s"leakage: ${split.split} features read $lastProfileDay, its rows start $firstRowDay"
      )

      val routes = RouteTransforms.edges(trips.where(w.covers(col("start_date"))), calendar, peaks)
      val graph  = GraphFeatures.compute(spark, stations, routes, graphCfg, strongRule(w))
      val base   = panel
        .where(split.rows.covers())
        .select(
          "station_id",
          "ts_hour",
          "date",
          "hour",
          "dow",
          "is_weekend",
          "is_holiday",
          "temp_c",
          "apparent_temp_c",
          "precip_mm",
          "wind_kmh",
          "humidity_pct",
          "cloud_pct",
          "is_raining",
          "departures",
          "arrivals",
          "net_flow"
        )
      val (hourSin, hourCos) = FeatureTransforms.hourOnCircle(col("hour"))
      val rows               = FeatureTransforms
        .withProfiles(base, FeatureTransforms.hourOfWeek(panel, w))
        .join(stationAttrs, Seq("station_id"), "left")
        .join(
          archetypes.where(col("window") === w.name).select("station_id", "archetype"),
          Seq("station_id"),
          "left"
        )
        .withColumn("archetype", coalesce(col("archetype"), lit(-1)))
        .withColumn("hour_sin", hourSin)
        .withColumn("hour_cos", hourCos)
        .withColumn("split", lit(split.split))
        .withColumn("is_validation", lit(split == ml.fit) && ml.validation.covers())
      Built(split, rows, graph, check)
    }
    val fitBuilt  = build(ml.fit)
    val testBuilt = build(ml.test)
    val built     = Seq(fitBuilt, testBuilt)

    // ---- One community numbering for both splits: the test window's districts take the fit window's ids.
    def communityMap(graph: DataFrame): Map[String, Int] =
      graph.select(
        "station_id",
        "community"
      ).as[(String, Int)].collect().toMap // bounded: one row per station
    val fitCommunities = communityMap(fitBuilt.graph)
    val alignment      = FeatureTransforms.alignCommunities(fitCommunities, communityMap(testBuilt.graph))
    val known          = fitCommunities.values.toSet
    val matched        = alignment.count { case (from, to) => from > 0 && known(to) }
    val fresh          = alignment.count(_._1 > 0) - matched
    val testGraph      = testBuilt.graph
      .join(broadcast(alignment.toSeq.toDF("community", "aligned")), Seq("community"), "left")
      .withColumn("community", coalesce(col("aligned"), lit(0)))
      .drop("aligned")

    def joinGraph(rows: DataFrame, graph: DataFrame): DataFrame =
      rows
        .join(graph, Seq("station_id"), "left")
        .na
        .fill(0.0, Seq("pagerank_w", "pr_am", "pr_pm", "pr_shift", "imbalance_ratio", "clustering_coef"))
        .na
        .fill(0L, Seq("in_strength", "out_strength"))
        .na
        .fill(0, Seq("community"))
    val fitRows  = joinGraph(fitBuilt.rows, fitBuilt.graph)
    val testRows = joinGraph(testBuilt.rows, testGraph)

    // ---- Tau, from the fit rows only.
    val tau = fitRows
      .select(abs(col("net_flow") / col("capacity")).as("relative"))
      .stat
      .approxQuantile("relative", Array(ml.regimeQuantile), 1e-4)
      .head
    log.info(
      f"regime threshold tau = $tau%.4f x capacity (quantile ${ml.regimeQuantile} of |net flow| / capacity)"
    )

    val features = Seq(fitRows, testRows)
      .map(_.withColumn("regime", FeatureTransforms.regime(col("net_flow"), col("capacity"), tau)))
      .reduce(_ unionByName _)
      .select(LakeSchemas.GoldMlFeature.fields.map(f => col(f.name).cast(f.dataType)): _*)
    features
      .repartition(col("date"))
      .write
      .mode(SaveMode.Overwrite)
      .option(
        "partitionOverwriteMode",
        "static"
      ) // a re-run with other windows must not leave old days behind
      .partitionBy("date")
      .parquet(lake.uri(LakeTable.MlFeatures))

    // ---- Checks and statistics on the written table.
    val written = lake.read(LakeTable.MlFeatures).persist(StorageLevel.MEMORY_AND_DISK)
    val columns = LakeSchemas.GoldMlFeature.fieldNames.toSeq
    val nulls   =
      written.select(columns.map(c => count(when(col(c).isNull, 1)).as(c)): _*).head() // bounded: 1 row
    val withNull = columns.filter(c => nulls.getAs[Long](c) > 0)
    require(withNull.isEmpty, s"null values in ${withNull.mkString(", ")}")

    val stats = written
      .groupBy("split")
      .agg(
        count(lit(1)).as("rows"),
        count(when(col("is_validation"), 1)).as("validationRows"),
        countDistinct("station_id").as("stations"),
        count(when(!col("has_profile"), 1)).as("withoutProfile"),
        count(when(col("archetype") < 0, 1)).as("withoutArchetype"),
        count(when(col("regime") === FeatureTransforms.Draining, 1)).as("draining"),
        count(when(col("regime") === FeatureTransforms.Stable, 1)).as("stable"),
        count(when(col("regime") === FeatureTransforms.Filling, 1)).as("filling"),
        avg("departures").as("meanDepartures")
      )
      .as[SplitStats]
      .collect() // bounded: 2 rows
      .sortBy(s => ml.splits.indexWhere(_.split == s.split))
      .map(s => s.copy(meanDepartures = round(s.meanDepartures, 4)))
      .toSeq

    def range(w: DateWindow) = SourceRange(w.from.toString, w.to.toString)
    val report               = FeatureReport(
      windows = ml.splits.map(s => SplitWindows(s.split, range(s.rows), range(s.profile))),
      validation = range(ml.validation),
      tau = round(tau, 5),
      tauQuantile = ml.regimeQuantile,
      strongMinTrips = strongRule(ml.fit.profile).minTrips,
      medianCapacity = median,
      communitiesMatched = matched,
      communitiesNew = fresh,
      leakage = built.map(_.check),
      splits = stats,
      features = Dictionary,
      targets = Targets
    )
    ServingJson.write(
      ctx.paths.serving.resolve("ml-features.json"),
      "ml-features",
      jobName,
      report,
      rows = Some(stats.map(_.rows).sum),
      source = Some(SourceRange(ml.fit.profile.from.toString, ml.test.rows.to.toString))
    )

    def share(n: Long, of: Long) = f"${100.0 * n / of}%.1f %%"
    stats.foreach { s =>
      log.info(
        f"${s.split}: ${s.rows}%,d rows, ${s.stations} stations; regime ${share(s.draining, s.rows)} draining, " +
          s"${share(s.stable, s.rows)} stable, ${share(s.filling, s.rows)} filling"
      )
    }
    Map(
      "fitRows"        -> stats.find(_.split == "fit").map(_.rows).getOrElse(0L),
      "validationRows" -> stats.find(_.split == "fit").map(_.validationRows).getOrElse(0L),
      "testRows"       -> stats.find(_.split == "test").map(_.rows).getOrElse(0L),
      "tau"            -> round(tau, 5),
      "regimeFit"      ->
        stats.find(_.split == "fit").map(s => s"${s.draining}/${s.stable}/${s.filling}").getOrElse(""),
      "regimeTest" ->
        stats.find(_.split == "test").map(s => s"${s.draining}/${s.stable}/${s.filling}").getOrElse(""),
      "leakageChecks" -> built.map(b =>
        s"${b.check.split}: ${b.check.lastProfileDay} < ${b.check.firstRowDay}"
      ).mkString("; "),
      "communitiesMatched" -> matched,
      "communitiesNew"     -> fresh,
      "strongMinTrips"     -> strongRule(ml.fit.profile).minTrips,
      "features"           -> Dictionary.size
    )
  }
}
