name := "stypes-flink"

version := "1.0"

scalaVersion := "2.12.6"

val flinkVersion = "1.7.0"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  ,"org.apache.flink" %% "flink-runtime-web" % flinkVersion
  ,"org.apache.flink" %% "flink-table" % flinkVersion
  ,"org.apache.calcite" % "calcite-example-csv" % "1.18.0"
  ,"org.apache.calcite" % "calcite-core" % "1.8.0"
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  ,"mysql" % "mysql-connector-java" % "5.1.46"
  )
