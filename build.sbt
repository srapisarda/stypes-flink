name := "stypes-flink"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.8-SNAPSHOT"
val calciteVersion = "1.22.0-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  ,"org.apache.flink" %% "flink-runtime-web" % flinkVersion
  ,"org.apache.flink" %% "flink-table" % flinkVersion
  ,"org.apache.calcite" % "calcite-example-csv" % calciteVersion
  ,"org.apache.calcite" % "calcite-core" % calciteVersion
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  ,"mysql" % "mysql-connector-java" % "5.1.46"
)
