package velograph.streaming

import scala.collection.JavaConverters._

import com.typesafe.config.Config

/** Topic names, as created by infra/kafka/create-topics.sh (ARCHITECTURE §9.2). */
final case class Topics(
    stationStatus: String,
    weather: String,
    tripsReplay: String,
    tripMetrics: String,
    predictions: String,
    alerts: String,
    dlq: String
)

/** `velograph.kafka`: the bootstrap servers of this mode, and the topics. */
final case class KafkaSettings(bootstrap: String, topics: Topics)

object KafkaSettings {
  def apply(raw: Config): KafkaSettings = {
    val c = raw.getConfig("velograph.kafka")
    val t = c.getConfig("topics")
    KafkaSettings(
      c.getString("bootstrap"),
      Topics(
        t.getString("station-status"),
        t.getString("weather"),
        t.getString("trips-replay"),
        t.getString("trip-metrics"),
        t.getString("predictions"),
        t.getString("alerts"),
        t.getString("dlq")
      )
    )
  }
}

/** The safety margin of a station: the fewest bikes, or free docks, it should keep. */
final case class RiskRule(minBikes: Int, capacityShare: Double) {
  def margin(capacity: Int): Int = math.max(minBikes, math.ceil(capacityShare * capacity).toInt)
}

/**
 * Limits of a suggested move (ARCHITECTURE §9.4): one van trip of at most `maxBikes`, within `maxKm`, and at
 * most `maxMoves` moves per plan.
 */
final case class RebalanceRule(maxKm: Double, maxBikes: Int, targetShare: Double, maxMoves: Int) {
  require(maxBikes > 0 && maxKm > 0 && maxMoves > 0, "a move must be able to carry bikes some distance")

  /** The bikes a station is brought back to (or down to): `targetShare` of its capacity. */
  def target(capacity: Int): Int = math.round(targetShare * capacity).toInt
}

/** `velograph.live`: the live scoring job (P5.3). */
final case class LiveConfig(
    trigger: String,
    watermark: String,
    maxOffsetsPerTrigger: Long,
    bootstrapMinutes: Int,
    risk: RiskRule,
    rebalance: RebalanceRule,
    defaultWeather: Map[String, Any],
    progressHistory: Int
)

object LiveConfig {
  def apply(raw: Config): LiveConfig = {
    val c = raw.getConfig("velograph.live")
    LiveConfig(
      trigger = c.getString("trigger"),
      watermark = c.getString("watermark"),
      maxOffsetsPerTrigger = c.getLong("max-offsets-per-trigger"),
      bootstrapMinutes = c.getInt("bootstrap-minutes"),
      risk = RiskRule(c.getInt("risk.min-bikes"), c.getDouble("risk.capacity-share")),
      rebalance = RebalanceRule(
        c.getDouble("rebalancing.max-km"),
        c.getInt("rebalancing.max-bikes"),
        c.getDouble("rebalancing.target-share"),
        c.getInt("rebalancing.max-moves")
      ),
      defaultWeather = c.getConfig("default-weather").root.unwrapped.asScala.toMap,
      progressHistory = c.getInt("progress-history")
    )
  }
}

/** `velograph.replay`: the trip replay analytics job (P5.4). */
final case class ReplayConfig(
    trigger: String,
    watermark: String,
    tumbling: String,
    slidingLength: String,
    slidingSlide: String,
    maxOffsetsPerTrigger: Long
)

object ReplayConfig {
  def apply(raw: Config): ReplayConfig = {
    val c = raw.getConfig("velograph.replay")
    ReplayConfig(
      c.getString("trigger"),
      c.getString("watermark"),
      c.getString("tumbling"),
      c.getString("sliding.length"),
      c.getString("sliding.slide"),
      c.getLong("max-offsets-per-trigger")
    )
  }
}
