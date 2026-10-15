package velograph.etl

import java.nio.file.Path

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper

/** Reads `data/raw/citibike/manifest.json`, written by `velograph.ingest.download_trips`. */
object RawManifest {

  private val mapper = new ObjectMapper()

  /** Month (`yyyymm`) -> number of data rows the downloader counted in its CSV files. */
  def rowsByMonth(path: Path): Map[String, Long] = {
    require(path.toFile.isFile, s"$path not found - run velograph.ingest.download_trips first")
    mapper
      .readTree(path.toFile)
      .path("months")
      .fields()
      .asScala
      .map(entry => entry.getKey -> entry.getValue.path("rows").asLong())
      .toMap
  }
}
