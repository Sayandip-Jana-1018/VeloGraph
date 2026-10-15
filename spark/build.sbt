ThisBuild / organization := "velograph"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18" // must match the Scala build of the Spark 3.5.8 runtime

val sparkVersion = "3.5.8"

// spark-submit adds these JDK 17 module flags itself (org.apache.spark.launcher.JavaModuleOptions);
// JVMs forked by sbt for tests and `run` need them spelled out.
val jdk17Opens = Seq(
  "java.lang",
  "java.lang.invoke",
  "java.lang.reflect",
  "java.io",
  "java.net",
  "java.nio",
  "java.util",
  "java.util.concurrent",
  "java.util.concurrent.atomic",
  "sun.nio.ch",
  "sun.nio.cs",
  "sun.security.action",
  "sun.util.calendar"
).map(pkg => s"--add-opens=java.base/$pkg=ALL-UNNAMED")

lazy val root = (project in file("."))
  .settings(
    name := "velograph-spark",
    libraryDependencies ++= Seq(
      // Provided: the cluster (or C:\spark) supplies Spark at runtime, so it stays out of the fat jar.
      "org.apache.spark" %% "spark-core"           % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql"            % sparkVersion % Provided,
      "org.apache.spark" %% "spark-mllib"          % sparkVersion % Provided,
      "org.apache.spark" %% "spark-graphx"         % sparkVersion % Provided,
      // Not part of the Spark distribution: bundled so streaming jobs run without --packages (and offline).
      // Its transitive Hadoop client and compression codecs ship with Spark already, so they stay out.
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion)
        .exclude("org.apache.hadoop", "hadoop-client-runtime")
        .exclude("org.apache.hadoop", "hadoop-client-api")
        .exclude("org.xerial.snappy", "snappy-java")
        .exclude("org.lz4", "lz4-java")
        .exclude("com.github.luben", "zstd-jni")
        .exclude("org.slf4j", "slf4j-api"),
      "com.typesafe"      % "config"               % "1.4.9",
      "com.github.scopt" %% "scopt"                % "4.2.0",
      "org.scalatest"    %% "scalatest"            % "3.2.20" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint", "-encoding", "UTF-8"),
    // `sbt run` needs the Provided Spark jars on its classpath and the repository root as working directory.
    Compile / run := Defaults
      .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
      .evaluated,
    run / fork          := true,
    run / javaOptions  ++= jdk17Opens :+ "-Xmx4g",
    run / baseDirectory := baseDirectory.value.getParentFile,
    Test / fork               := true,
    Test / javaOptions       ++= jdk17Opens :+ "-Xmx2g",
    Test / parallelExecution  := false,
    assembly / assemblyJarName := "velograph-spark.jar",
    // One jar, many jobs: spark-submit always names the --class, so no manifest main class is needed.
    assembly / mainClass             := None,
    Compile / packageBin / mainClass := None,
    assemblyPackageScala / assembleArtifact := false, // the Spark runtime provides scala-library 2.12
    assembly / assemblyMergeStrategy := {
      // Keep service registrations: they are how Spark discovers the "kafka" data source.
      case PathList("META-INF", "services", _ @_*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", _ @_*)             => MergeStrategy.discard
      case path if path.endsWith("module-info.class") => MergeStrategy.discard
      case "reference.conf"                        => MergeStrategy.concat
      case _                                       => MergeStrategy.first
    }
  )
