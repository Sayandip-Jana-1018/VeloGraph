package velograph.common

import java.nio.file.Paths

import scala.collection.JavaConverters._

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Settings for one job run, resolved from three layers (highest priority first):
 *
 *   1. `VELOGRAPH_*` environment variables (and JVM system properties);
 *   2. the mode overlay `conf/<mode>.conf`, where mode is `dev` (host) or `cluster` (Docker);
 *   3. the defaults in `application.conf`.
 *
 * Spark properties passed to spark-submit or found in spark-defaults.conf still win over all three; see
 * [[SparkSessionFactory]].
 */
final case class AppConfig(
    mode: String,
    paths: LakePaths,
    sparkMaster: String,
    sparkConf: Map[String, String],
    raw: Config
)

object AppConfig {

  /** Environment variables and the config path each one overrides. */
  private val EnvOverrides: Map[String, String] = Map(
    "VELOGRAPH_MODE"         -> "velograph.mode",
    "VELOGRAPH_DATA_ROOT"    -> "velograph.data-root",
    "VELOGRAPH_SPARK_MASTER" -> "velograph.spark.master"
  )

  def load(env: Map[String, String] = sys.env): AppConfig = {
    val defaults = ConfigFactory.parseResources("application.conf")
    val envLayer = ConfigFactory.parseMap(
      EnvOverrides.collect { case (variable, path) if env.contains(variable) => path -> env(variable) }.asJava
    )
    val mode        = envLayer.withFallback(defaults).getString("velograph.mode")
    val overlayFile = Paths.get(env.getOrElse("VELOGRAPH_CONF_DIR", "conf")).resolve(s"$mode.conf").toFile
    val overlay     = if (overlayFile.isFile) ConfigFactory.parseFile(overlayFile) else ConfigFactory.empty()

    val config = ConfigFactory
      .defaultOverrides()
      .withFallback(envLayer)
      .withFallback(overlay)
      .withFallback(defaults)
      .resolve()
    val velograph = config.getConfig("velograph")

    AppConfig(
      mode = mode,
      paths = LakePaths(Paths.get(velograph.getString("data-root")).toAbsolutePath.normalize()),
      sparkMaster = velograph.getString("spark.master"),
      sparkConf = velograph
        .getObject("spark.conf")
        .asScala
        .map { case (key, value) => key -> value.unwrapped.toString }
        .toMap,
      raw = config
    )
  }
}
