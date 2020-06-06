ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "kamu-engine-flink"
version := "0.1-SNAPSHOT"
organization := "dev.kamu"
ThisBuild / scalaVersion := "2.12.11"

//////////////////////////////////////////////////////////////////////////////
// Projects
//////////////////////////////////////////////////////////////////////////////

// Don't run tests in parallel
ThisBuild / parallelExecution := false

lazy val root = (project in file("."))
  .aggregate(
    kamuCoreUtils,
    kamuCoreManifests
  )
  .dependsOn(
    kamuCoreUtils,
    kamuCoreManifests
  )
  //.enablePlugins(AutomateHeaderPlugin)
  .settings(
    aggregate in assembly := false,
    test in assembly := {},
    //assemblySettings
    libraryDependencies ++= flinkDependencies
  )

lazy val kamuCoreUtils = project
  .in(file("core.utils"))
  //.enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.hadoopCommon % "provided",
      deps.scalaTest % "test"
      //deps.sparkCore % "provided",
      //deps.sparkHive % "provided",
      //deps.geoSpark % "test",
      //deps.geoSparkSql % "test",
      //deps.sparkTestingBase % "test",
      //deps.sparkHive % "test"
    )
    //commonSettings,
    //sparkTestingSettings
  )

lazy val kamuCoreManifests = project
  .in(file("core.manifests"))
  .dependsOn(
    kamuCoreUtils % "compile->compile;test->test"
  )
  //.enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.hadoopCommon % "provided",
      deps.pureConfig,
      deps.pureConfigYaml,
      deps.spire
    )
    //commonSettings
  )

//////////////////////////////////////////////////////////////////////////////
// Dependencies
//////////////////////////////////////////////////////////////////////////////

lazy val versions = new {
  val geoSpark = "1.2.0"
  val hadoopCommon = "2.6.5"
  val pureConfig = "0.11.1"
  val spark = "2.4.0"
  val sparkTestingBase = s"${spark}_0.12.0"
  val spire = "0.13.0" // Used by spark too
}

lazy val deps =
  new {
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Spark
    val sparkCore = "org.apache.spark" %% "spark-core" % versions.spark
    val sparkSql = "org.apache.spark" %% "spark-sql" % versions.spark
    // GeoSpark
    val geoSpark = "org.datasyslab" % "geospark" % versions.geoSpark
    val geoSparkSql = "org.datasyslab" % "geospark-sql_2.3" % versions.geoSpark
    // Hadoop File System
    val hadoopCommon =
      ("org.apache.hadoop" % "hadoop-common" % versions.hadoopCommon)
        .exclude("commons-beanutils", "commons-beanutils")
        .exclude("commons-beanutils", "commons-beanutils-core")
    // Math
    // TODO: Using older version as it's also used by Spark
    //val spire = "org.typelevel" %% "spire" % versions.spire
    val spire = "org.spire-math" %% "spire" % versions.spire
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
    val sparkHive = "org.apache.spark" %% "spark-hive" % versions.spark
    val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % versions.sparkTestingBase
  }

val flinkVersion = "1.10.1"

val flinkDependencies = Seq(
  //"org.slf4j" % "slf4j-simple" % "1.7.30" % "provided",
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-planner" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-planner-blink" % flinkVersion % "provided",
  //"org.apache.flink" % "flink-csv" % flinkVersion % "provided",
  "org.apache.flink" % "flink-avro" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-parquet" % flinkVersion,
  "org.apache.parquet" % "parquet-avro" % "1.10.0",
  ("org.apache.hadoop" % "hadoop-client" % "2.8.3")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % "test",
  "org.apache.flink" %% "flink-runtime" % flinkVersion % "test",
  // TODO: newer avro breaks flink serialization?
  "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4" % "test"
)

// make run command include the provided dependencies
Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
//Compile / run / fork := true
Global / cancelable := true

assembly / mainClass := Some("dev.kamu.engine.flink.EngineApp")
assembly / assemblyJarName := "engine.flink.jar"

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value
  .copy(includeScala = false)
