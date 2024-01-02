package dev.kamu.engine.flink.test

import com.sksamuel.avro4s.ScalePrecisionRoundingMode

import java.sql.Timestamp
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.TransformRequest
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.math.BigDecimal.RoundingMode

case class WriteRaw(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  value: String
) extends HasOffset {
  override def getOffset: Long = offset
}

case class WriteResult(
  system_time: Timestamp,
  event_time: Timestamp,
  decimal_13_4: Option[BigDecimal],
  decimal_38_18: Option[BigDecimal]
)

case class ReadInput(
  offset: Long,
  system_time: Timestamp,
  event_time: Timestamp,
  decimal: BigDecimal
) extends HasOffset {
  override def getOffset: Long = offset
}

case class ReadOutput(
  system_time: Timestamp,
  event_time: Timestamp,
  // TODO: need to upgrade avro4s version
  // decimal: BigDecimal
  decimal: String
)

class EngineFormatsTest
    extends FunSuite
    with Matchers
    with BeforeAndAfter
    with TimeHelpers
    with EngineHelpers {

  test("Test write decimal") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[TransformRequest](
        s"""
           |datasetId: "did:odf:blah"
           |datasetAlias: out
           |systemTime: "2020-01-01T00:00:00Z"
           |nextOffset: 0
           |transform:
           |  kind: Sql
           |  engine: flink
           |  query: |
           |    SELECT
           |      event_time,
           |      TRY_CAST(`value` as DECIMAL(13,4)) as decimal_13_4,
           |      TRY_CAST(`value` as DECIMAL(38,18)) as decimal_38_18
           |    FROM `in`
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  systemTimeColumn: system_time
           |  eventTimeColumn: event_time
           |""".stripMargin
      )

      var request = withRandomOutputPath(requestTemplate, outputLayout)
      request = withInputData(
        request,
        "in",
        inputLayout.dataDir,
        Seq(
          WriteRaw(0, ts(1), ts(1, 1), "123456789.0123"),
          WriteRaw(1, ts(1), ts(1, 2), "-123456789.0123"),
          WriteRaw(
            2,
            ts(1),
            ts(1, 3),
            "12345678901234567890.123456789012345678"
          ),
          WriteRaw(
            3,
            ts(1),
            ts(1, 4),
            "-12345678901234567890.123456789012345678"
          )
        )
      )

      engineRunner.executeTransform(
        withWatermarks(request, Map("in" -> ts(3, 2)))
          .copy(systemTime = ts(10).toInstant),
        tempDir
      )

      val schema = ParquetHelpers.getSchemaFromFile(request.newDataPath)
      schema.toString shouldEqual
        """message org.apache.flink.avro.generated.record {
          |  required int64 offset;
          |  required int64 system_time (TIMESTAMP(MILLIS,true));
          |  required int64 event_time (TIMESTAMP(MILLIS,true));
          |  optional binary decimal_13_4 (DECIMAL(13,4));
          |  optional binary decimal_38_18 (DECIMAL(38,18));
          |}
          |""".stripMargin

      val actual = ParquetHelpers
        .read[WriteResult](request.newDataPath)
        .sortBy(i => i.event_time.getTime)

      actual shouldEqual List(
        WriteResult(
          ts(10),
          ts(1, 1),
          Some(BigDecimal("123456789.0123")),
          Some(BigDecimal("123456789.0123"))
        ),
        WriteResult(
          ts(10),
          ts(1, 2),
          Some(BigDecimal("-123456789.0123")),
          Some(BigDecimal("-123456789.0123"))
        ),
        WriteResult(
          ts(10),
          ts(1, 3),
          None,
          Some(BigDecimal("12345678901234567890.123456789012345678"))
        ),
        WriteResult(
          ts(10),
          ts(1, 4),
          None,
          Some(BigDecimal("-12345678901234567890.123456789012345678"))
        )
      )
    }
  }

  test("Test read decimal") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputLayout = tempLayout(tempDir, "in")
      val outputLayout = tempLayout(tempDir, "out")

      val requestTemplate = yaml.load[TransformRequest](
        s"""
           |datasetId: "did:odf:blah"
           |datasetAlias: out
           |systemTime: "2020-01-01T00:00:00Z"
           |nextOffset: 0
           |transform:
           |  kind: Sql
           |  engine: flink
           |  query: |
           |    SELECT event_time, cast(`decimal` as string) as `decimal` FROM `in`
           |queryInputs: []
           |newCheckpointPath: ""
           |newDataPath: ""
           |vocab:
           |  offsetColumn: offset
           |  systemTimeColumn: system_time
           |  eventTimeColumn: event_time
           |""".stripMargin
      )

      implicit val sp =
        ScalePrecisionRoundingMode(4, 20, RoundingMode.UNNECESSARY)

      var request = withRandomOutputPath(requestTemplate, outputLayout)
      request = withInputData(
        request,
        "in",
        inputLayout.dataDir,
        Seq(
          ReadInput(0, ts(1), ts(1, 1), BigDecimal("123456789.0123"))
        )
      )

      engineRunner.executeTransform(
        withWatermarks(request, Map("in" -> ts(3, 2)))
          .copy(systemTime = ts(10).toInstant),
        tempDir
      )

      val actual = ParquetHelpers
        .read[ReadOutput](request.newDataPath)
        .sortBy(i => i.event_time.getTime)

      actual shouldEqual List(
        ReadOutput(ts(10), ts(1, 1), "123456789.0123")
      )
    }
  }
}
