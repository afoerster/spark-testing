name := "sparkTesting"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)