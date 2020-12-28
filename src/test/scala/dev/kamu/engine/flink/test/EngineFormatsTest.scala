package dev.kamu.engine.flink.test

import com.sksamuel.avro4s.ScalePrecisionRoundingMode

import java.nio.file.Paths
import java.sql.Timestamp
import pureconfig.generic.auto._
import dev.kamu.core.manifests._
import dev.kamu.core.manifests.parsing.pureconfig.yaml
import dev.kamu.core.manifests.parsing.pureconfig.yaml.defaults._
import dev.kamu.core.manifests.infra.ExecuteQueryRequest
import dev.kamu.core.utils.DockerClient
import dev.kamu.core.utils.fs._
import dev.kamu.core.utils.Temp
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.math.BigDecimal.RoundingMode

case class WriteRaw(
  system_time: Timestamp,
  event_time: Timestamp,
  value: String
)

case class WriteResult(
  system_time: Timestamp,
  event_time: Timestamp,
  decimal_13_4: Option[BigDecimal],
  decimal_38_18: Option[BigDecimal]
)

case class ReadInput(
  system_time: Timestamp,
  event_time: Timestamp,
  decimal: BigDecimal
)

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

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: out
           |source:
           |  inputs:
           |    - in
           |  transform:
           |    kind: sql
           |    engine: flink
           |    query: >
           |      SELECT
           |        event_time,
           |        CAST(`value` as DECIMAL(13,4)) as decimal_13_4,
           |        CAST(`value` as DECIMAL(38,18)) as decimal_38_18
           |      FROM `in`
           |inputSlices: {}
           |newCheckpointDir: ""
           |outDataPath: ""
           |datasetVocabs:
           |  in: {}
           |  out: {}
           |""".stripMargin
      )

      var request = withRandomOutputPath(requestTemplate, outputLayout)
      request = withInputData(
        request,
        "in",
        inputLayout.dataDir,
        Seq(
          WriteRaw(ts(1), ts(1, 1), "123456789.0123"),
          WriteRaw(ts(1), ts(1, 2), "-123456789.0123"),
          WriteRaw(ts(1), ts(1, 3), "12345678901234567890.123456789012345678"),
          WriteRaw(ts(1), ts(1, 4), "-12345678901234567890.123456789012345678")
        )
      )

      engineRunner.run(
        withWatermarks(request, Map("in" -> ts(3, 2))),
        tempDir,
        ts(10)
      )

      val actual = ParquetHelpers
        .read[WriteResult](Paths.get(request.outDataPath))
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

      val requestTemplate = yaml.load[ExecuteQueryRequest](
        s"""
           |datasetID: out
           |source:
           |  inputs:
           |    - in
           |  transform:
           |    kind: sql
           |    engine: flink
           |    query: >
           |      SELECT event_time, cast(`decimal` as string) as `decimal` FROM `in`
           |inputSlices: {}
           |newCheckpointDir: ""
           |outDataPath: ""
           |datasetVocabs:
           |  in: {}
           |  out: {}
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
          ReadInput(ts(1), ts(1, 1), BigDecimal("123456789.0123"))
        )
      )

      engineRunner.run(
        withWatermarks(request, Map("in" -> ts(3, 2))),
        tempDir,
        ts(10)
      )

      val actual = ParquetHelpers
        .read[ReadOutput](Paths.get(request.outDataPath))
        .sortBy(i => i.event_time.getTime)

      actual shouldEqual List(
        ReadOutput(ts(10), ts(1, 1), "123456789.0123")
      )
    }
  }
}
