package velograph.common

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

class AppConfigSpec extends AnyFunSuite {

  private val noOverlay = "VELOGRAPH_CONF_DIR" -> "no-such-dir"

  test("defaults: dev mode, data under the working directory, New York session time zone") {
    val cfg = AppConfig.load(env = Map(noOverlay))
    assert(cfg.mode == "dev")
    assert(cfg.paths.root == Paths.get("data").toAbsolutePath.normalize())
    assert(cfg.sparkConf("spark.sql.session.timeZone") == "America/New_York")
  }

  test("environment variables override the data root and the master") {
    val root = Files.createTempDirectory("velograph-lake")
    val cfg  = AppConfig.load(
      env = Map("VELOGRAPH_DATA_ROOT" -> root.toString, "VELOGRAPH_SPARK_MASTER" -> "local[1]", noOverlay)
    )
    assert(cfg.paths.gold == root.toAbsolutePath.normalize().resolve("lake").resolve("gold"))
    assert(cfg.sparkMaster == "local[1]")
  }

  test("a mode overlay replaces individual Spark settings and keeps the rest") {
    val dir = Files.createTempDirectory("velograph-conf")
    Files.write(
      dir.resolve("cluster.conf"),
      "velograph.spark.conf { \"spark.sql.shuffle.partitions\" = \"8\" }".getBytes(StandardCharsets.UTF_8)
    )
    val cfg = AppConfig.load(env = Map("VELOGRAPH_MODE" -> "cluster", "VELOGRAPH_CONF_DIR" -> dir.toString))
    assert(cfg.mode == "cluster")
    assert(cfg.sparkConf("spark.sql.shuffle.partitions") == "8")
    assert(cfg.sparkConf("spark.sql.session.timeZone") == "America/New_York")
  }
}
