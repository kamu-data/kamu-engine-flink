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
      deps.betterFiles,
      deps.log4jApi,
      deps.scalaTest % "test"
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
      deps.betterFiles,
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
  val log4j = "2.13.3"
  val betterFiles = "3.9.1"
  val flink = "1.12-SNAPSHOT"
  val pureConfig = "0.13.0"
  val spire = "0.13.0" // Used by spark too
}

lazy val deps =
  new {
    val log4jApi = "org.apache.logging.log4j" % "log4j-api" % versions.log4j
    // File System
    val betterFiles = "com.github.pathikrit" %% "better-files" % versions.betterFiles
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Math
    // TODO: Using older version as it's also used by Spark
    //val spire = "org.typelevel" %% "spire" % versions.spire
    val spire = "org.spire-math" %% "spire" % versions.spire
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  }

val flinkDependencies = Seq(
  deps.log4jApi,
  deps.betterFiles,
  "org.apache.flink" %% "flink-scala" % versions.flink % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % versions.flink % "provided",
  "org.apache.flink" %% "flink-table-api-scala-bridge" % versions.flink % "provided",
  // TODO: Tests won't run without this ... are we using blink or not?
  "org.apache.flink" %% "flink-table-planner" % versions.flink % "test",
  "org.apache.flink" %% "flink-table-planner-blink" % versions.flink % "provided",
  //"org.apache.flink" % "flink-csv" % flinkVersion % "provided",
  "org.apache.flink" % "flink-avro" % versions.flink % "provided",
  "org.apache.flink" %% "flink-parquet" % versions.flink,
  "org.apache.parquet" % "parquet-avro" % "1.10.0",
  ("org.apache.hadoop" % "hadoop-client" % "2.8.3")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.flink" %% "flink-test-utils" % versions.flink % "test",
  "org.apache.flink" %% "flink-runtime" % versions.flink % "test",
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
