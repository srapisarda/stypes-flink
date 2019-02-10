name := "stypes-flink"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.8-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  ,"org.apache.flink" %% "flink-runtime-web" % flinkVersion
  ,"org.apache.flink" %% "flink-table" % flinkVersion
  ,"org.apache.calcite" % "calcite-example-csv" % "1.19.0-SNAPSHOT"
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  )
