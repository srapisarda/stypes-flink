name := "stypes-flink"

version := "1.0"

scalaVersion := "2.12.10"

val flinkVersion = "1.13.3"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  ,"org.apache.flink" %% "flink-runtime-web" % flinkVersion
  ,"org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion
  ,"org.apache.flink" %% "flink-table-planner-blink" % flinkVersion
  ,"org.apache.flink" %% "flink-table-planner" %  flinkVersion
  ,"org.apache.flink"  % "flink-hadoop-fs" % flinkVersion
  ,"org.apache.flink" % "flink-s3-fs-hadoop" % flinkVersion
  ,"com.typesafe" % "config" % "1.4.0"
  ,"com.google.guava" % "guava" % "28.0-jre"
  ,"com.google.code.gson" % "gson" % "2.8.6"
  ,"org.scalatest" %% "scalatest" % "3.0.4" % "test"
  ,"junit" % "junit" % "4.10" % "test"
  ,"ch.qos.logback" % "logback-classic" % "1.2.3"
  ,"mysql" % "mysql-connector-java" % "5.1.46"
  //,"com.github.srapisarda" %% "stypes" %  "1.1.1"
)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { f =>
    f.data.getName.contains("flink") || f.data.getName.contains("common") || f.data.getName.contains("logback")
  }
}