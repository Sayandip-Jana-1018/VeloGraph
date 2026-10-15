package velograph.common

import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.Instant

import scala.collection.immutable.ListMap

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** Inclusive date range of the data an export was computed from. */
final case class SourceRange(from: String, to: String)

/**
 * Writes the JSON files the website and the report read (ARCHITECTURE §10). Every file has the same envelope
 * — `meta` (schema, version, generation time, producing job, source range, row count) and `data` — and is
 * replaced atomically, so a reader never sees half a file.
 */
object ServingJson {

  private val mapper =
    new ObjectMapper().registerModule(DefaultScalaModule).enable(SerializationFeature.INDENT_OUTPUT)

  def write(
      target: Path,
      schema: String,
      job: String,
      data: Any,
      rows: Option[Long] = None,
      source: Option[SourceRange] = None
  ): Path = {
    val meta = ListMap[String, Any](
      "schema"      -> s"velograph.$schema",
      "version"     -> 1,
      "generatedAt" -> Instant.now().toString,
      "job"         -> job
    ) ++ source.map(range => "source" -> range) ++ rows.map(count => "rows" -> count)

    Files.createDirectories(target.getParent)
    val tmp = target.resolveSibling(s".${target.getFileName}.tmp")
    mapper.writeValue(tmp.toFile, ListMap[String, Any]("meta" -> meta, "data" -> data))
    Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
  }
}
