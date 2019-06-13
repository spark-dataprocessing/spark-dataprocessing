name := "spark-dataprocessing"

version := "0.1"

scalaVersion := "2.11.12"



val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.rogach" %% "scallop" % "3.2.0"
)


// POM settings for Sonatype
organization := "io.github.spark-dataprocessing"
homepage := Some(url("https://github.com/spark-dataprocessing/spark-dataprocessing"))
scmInfo := Some(ScmInfo(url("https://github.com/spark-dataprocessing/spark-dataprocessing"),
"git@github.com:spark-dataprocessing/spark-dataprocessing.git"))
developers := List(  Developer(
  id    = "Your identifier",
  name  = "Kaspar Mosinger",
  email = "mokaspar@gmail.com",
  url   = url("https://github.com/mokaspar")
))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)
