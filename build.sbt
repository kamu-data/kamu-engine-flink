ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "kamu-engine-flink"
version := "0.1-SNAPSHOT"
organization := "dev.kamu"
ThisBuild / scalaVersion := "2.12.13"

//////////////////////////////////////////////////////////////////////////////
// Projects
//////////////////////////////////////////////////////////////////////////////

// Don't run tests in parallel
ThisBuild / parallelExecution := false

// Due to IntelliJ bug
ThisBuild / useCoursier := false

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
    assembly / aggregate := false,
    assembly / test := {},
    //assemblySettings
    libraryDependencies ++= flinkDependencies
  )

lazy val kamuCoreUtils = project
  .in(file("core.utils"))
  //.enablePlugins(AutomateHeaderPlugin)
  .settings(
    libraryDependencies ++= Seq(
      deps.betterFiles,
      deps.slf4jApi,
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
  val betterFiles = "3.9.1"
  val flink = "1.16.0"
  val pureConfig = "0.13.0"
  val spire = "0.13.0"
}

lazy val deps =
  new {
    val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.36"
    // File System
    val betterFiles = "com.github.pathikrit" %% "better-files" % versions.betterFiles
    // Configs
    val pureConfig = "com.github.pureconfig" %% "pureconfig" % versions.pureConfig
    val pureConfigYaml = "com.github.pureconfig" %% "pureconfig-yaml" % versions.pureConfig
    // Math
    val spire = "org.spire-math" %% "spire" % versions.spire
    // Test
    val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"
  }

val flinkDependencies = Seq(
  deps.slf4jApi,
  deps.betterFiles,
  "org.apache.flink" %% "flink-scala" % versions.flink % "provided",
  //"org.apache.flink" %% "flink-streaming-scala" % versions.flink % "provided",
  //"org.apache.flink" %% "flink-table-api-scala" % versions.flink,
  "org.apache.flink" % "flink-table-runtime" % versions.flink % "provided",
  "org.apache.flink" %% "flink-table-api-scala-bridge" % versions.flink,
  "org.apache.flink" %% "flink-table-planner" % versions.flink % "test",
  "org.apache.flink" % "flink-connector-files" % versions.flink % "provided",
  "org.apache.flink" % "flink-avro" % versions.flink,
  "org.apache.flink" % "flink-parquet" % versions.flink,
  ("org.apache.parquet" % "parquet-avro" % "1.12.3")
    .exclude("org.apache.hadoop", "hadoop-client")
    .exclude("it.unimi.dsi", "fastutil"),
  ("org.apache.hadoop" % "hadoop-client" % "2.8.3")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core"),
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.flink" % "flink-test-utils" % versions.flink % "test",
  "org.apache.flink" % "flink-runtime" % versions.flink % "test",
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

assembly / assemblyJarName := "engine.flink.jar"

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value
  .copy(includeScala = false)

assembly / assemblyMergeStrategy := {
  case "module-info.class"                     => MergeStrategy.discard
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
