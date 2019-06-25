// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "io.github.spark-dataprocessing"


organization := "io.github.spark-dataprocessing"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// License of your choice
licenses := Seq("APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt"))

// Where is the source code hosted
// or if you want to set these fields manually
homepage := Some(url("https://github.com/spark-dataprocessing/spark-dataprocessing"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/spark-dataprocessing/spark-dataprocessing"),
    "scm:git@github.com:spark-dataprocessing/spark-dataprocessing.git"
  )
)
developers := List(
  Developer(id="mokaspar", name="Kaspar Moesinger", email="mokaspar@gmail.com", url=url("https://github.com/mokaspar"))
)

