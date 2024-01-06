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

case class ValueRaw(
  value: String
)

case class ValueDecimalPrecision(
  decimal_13_4: Option[BigDecimal],
  decimal_38_18: Option[BigDecimal]
)

case class ValueDecimal(
  decimal: BigDecimal
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

      val inputPath = tempDir.resolve("in")
      val outputPath = tempDir.resolve("out")

      val request = yaml.load[RawQueryRequest](
        s"""
           |inputDataPaths:
           |- $inputPath
           |transform:
           |  kind: Sql
           |  engine: flink
           |  queries:
           |  - query: |
           |      SELECT
           |        TRY_CAST(`value` as DECIMAL(13,4)) as decimal_13_4,
           |        TRY_CAST(`value` as DECIMAL(38,18)) as decimal_38_18
           |      FROM `input`
           |outputDataPath: $outputPath
           |""".stripMargin
      )

      ParquetHelpers.write(
        inputPath,
        Seq(
          ValueRaw("123456789.0123"),
          ValueRaw("-123456789.0123"),
          ValueRaw("12345678901234567890.123456789012345678"),
          ValueRaw("-12345678901234567890.123456789012345678")
        )
      )

      engineRunner.executeRawQuery(request, tempDir)

      val schema = ParquetHelpers.getSchemaFromFile(outputPath)
      schema.toString shouldEqual
        """message org.apache.flink.avro.generated.record {
          |  optional binary decimal_13_4 (DECIMAL(13,4));
          |  optional binary decimal_38_18 (DECIMAL(38,18));
          |}
          |""".stripMargin

      val actual = ParquetHelpers.read[ValueDecimalPrecision](outputPath)

      actual shouldEqual List(
        ValueDecimalPrecision(
          Some(BigDecimal("123456789.0123")),
          Some(BigDecimal("123456789.0123"))
        ),
        ValueDecimalPrecision(
          Some(BigDecimal("-123456789.0123")),
          Some(BigDecimal("-123456789.0123"))
        ),
        ValueDecimalPrecision(
          None,
          Some(BigDecimal("12345678901234567890.123456789012345678"))
        ),
        ValueDecimalPrecision(
          None,
          Some(BigDecimal("-12345678901234567890.123456789012345678"))
        )
      )
    }
  }

  test("Test read decimal") {
    Temp.withRandomTempDir("kamu-engine-flink") { tempDir =>
      val engineRunner = new EngineRunner(new DockerClient())

      val inputPath = tempDir.resolve("in")
      val outputPath = tempDir.resolve("out")

      val request = yaml.load[RawQueryRequest](
        s"""
           |inputDataPaths:
           |- $inputPath
           |transform:
           |  kind: Sql
           |  engine: flink
           |  queries:
           |  - query: |
           |      SELECT
           |        cast(`decimal` as string) as `value`
           |      FROM `input`
           |outputDataPath: $outputPath
           |""".stripMargin
      )

      implicit val sp =
        ScalePrecisionRoundingMode(4, 20, RoundingMode.UNNECESSARY)

      ParquetHelpers.write(
        inputPath,
        Seq(
          ValueDecimal(BigDecimal("123456789.0123"))
        )
      )

      engineRunner.executeRawQuery(request, tempDir)

      val actual = ParquetHelpers
        .read[ValueRaw](outputPath)

      actual shouldEqual List(
        ValueRaw("123456789.0123")
      )
    }
  }
}
