name := "spark-dataprocessing"

version := "0.1"

scalaVersion := "2.11.12"



val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.rogach" %% "scallop" % "3.2.0"
)


publishTo := sonatypePublishTo.value

