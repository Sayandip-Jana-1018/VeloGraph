package velograph.common

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.graphx.GraphXUtils
import org.apache.spark.sql.SparkSession

import velograph.common.LakePaths.PathOps

object SparkSessionFactory {

  /**
   * Builds the session for a job. VeloGraph settings only fill gaps (`setIfMissing`), so whatever was given
   * to spark-submit (`--master`, `--conf`) or found in spark-defaults.conf takes precedence.
   */
  def create(appName: String, cfg: AppConfig): SparkSession = {
    // The event log directory must exist before the SparkContext starts.
    Files.createDirectories(cfg.paths.sparkEvents)

    val conf = new SparkConf(loadDefaults = true).setAppName(appName)
    cfg.sparkConf.foreach { case (key, value) => conf.setIfMissing(key, value) }
    conf.setIfMissing("spark.eventLog.dir", cfg.paths.sparkEvents.uri)
    conf.setIfMissing("spark.master", cfg.sparkMaster)
    GraphXUtils.registerKryoClasses(conf) // GraphX internals serialize far smaller with Kryo registration

    SparkSession.builder().config(conf).getOrCreate()
  }
}
