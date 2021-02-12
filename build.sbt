import AssemblyKeys._

name := "BookingInfoSparkStreaming"
scalaVersion := "2.11.7"
version := "1.0.0"
val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.typesafe" % "config" % "1.3.1",
  "org.json4s" %% "json4s-core" % "3.5.3",
  "org.json4s" %% "json4s-jackson" % "3.5.3",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalamock" %% "scalamock" % "4.1.0" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test
)

assemblySettings

mergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x => (mergeStrategy in assembly).value(x)
}
