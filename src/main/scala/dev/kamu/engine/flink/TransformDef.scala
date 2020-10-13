package dev.kamu.engine.flink

import dev.kamu.core.manifests.DatasetID

case class TransformDef(
  kind: String,
  engine: String,
  version: Option[String],
  /** Specifies which input streams should be treated as temporal tables.
    * See: https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/temporal_tables.html
    */
  temporalTables: Vector[TransformDef.TemporalTable] = Vector.empty,
  /** Processing steps that shape the data */
  queries: Vector[TransformDef.Query] = Vector.empty,
  /** Convenience way to provide a single SQL statement with no alias **/
  query: Option[String]
)

object TransformDef {
  case class TemporalTable(
    /** ID of the input dataset */
    id: DatasetID,
    /** When specified primary key of the temporal table
      */
    primaryKey: Vector[String] = Vector.empty
  )

  case class Query(
    /** An alias given to the result of this step that can be used to referred to it in the later steps.
      * Acts as a shorthand for `CREATE TEMPORARY VIEW <alias> AS (<query>)`.
      */
    alias: Option[String] = None,
    /** An SQL statement **/
    query: String
  )
}
