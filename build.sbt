import AssemblyKeys._

name := "spark-decompositions"

version := "0.1.0"

scalaVersion := "2.9.3"

scalacOptions += "-optimise"

// parallel execution of tests is disabled since it causes problems with
// multiple SparkContexts
parallelExecution in Test := false

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1"

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3" excludeAll(
    ExclusionRule("ch.qos.logback"),
    ExclusionRule("org.apache.hadoop")
    )

libraryDependencies += "it.unimi.dsi" % "webgraph" % "3.0.9" exclude("ch.qos.logback", "logback-classic")

libraryDependencies += "org.scalatest" % "scalatest_2.9.3" % "1.9.1" exclude("ch.qos.logback", "logback-classic")

libraryDependencies += "org.rogach" %% "scallop" % "0.9.4"

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")


// sbt-assembly configuration

assemblySettings

mainClass in assembly := Some("it.unipd.dei.graph.Tool")

test in assembly := {} // skip tests

// directly from the sbt-assembly documentation
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>  {
    case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*)  => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".xml" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
    case PathList(ps @ _*) if ps.last endsWith "log4j.properties" => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case PathList(ps @ _*) if ps.last endsWith ".properties" => MergeStrategy.concat
    case "unwanted.txt"     => MergeStrategy.discard
    case x => old(x)
  }
}

