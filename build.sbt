name := "stypes-flink"

version := "1.0"

scalaVersion := "2.11.8"

val flinkVersion = "1.10-SNAPSHOT"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  ,"org.apache.flink" %% "flink-runtime-web" % flinkVersion
  ,"org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion
  ,"org.apache.flink" %% "flink-table-planner-blink" % flinkVersion
  ,"org.apache.flink" %% "flink-table-planner" % flinkVersion
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  ,"mysql" % "mysql-connector-java" % "5.1.46"
)
