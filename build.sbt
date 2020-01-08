name := "spark-dataprocessing"

version := "0.2.0"

scalaVersion := "2.11.12"



val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)


publishTo := sonatypePublishTo.value

