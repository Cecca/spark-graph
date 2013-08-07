name := "spark-decompositions"

version := "0.1"

scalaVersion := "2.9.3"

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3"

libraryDependencies += "it.unimi.dsi" % "webgraph" % "3.0.9"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")

