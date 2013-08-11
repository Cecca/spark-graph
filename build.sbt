name := "spark-decompositions"

version := "0.1"

scalaVersion := "2.9.3"

// parallel execution of tests is disabled since it causes problems with
// multiple SparkContexts
parallelExecution in Test := false

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3"

libraryDependencies += "it.unimi.dsi" % "webgraph" % "3.0.9"

libraryDependencies += "org.scalatest" % "scalatest_2.9.3" % "1.9.1"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")


