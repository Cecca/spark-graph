package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._
import spark.{RDD, SparkContext}
import SparkContext._

class BigGraphBallDecompositionSpec extends FlatSpec with OneInstancePerTest
                                                     with BeforeAndAfter {

  // --------------------------------------------------------------------------
  // Paths
  val graphFile = "src/test/resources/big/graph.adj"
  val ballsFile = "src/test/resources/big/balls_cardinalities"
  val colorsFile = "src/test/resources/big/colors"
  val centersFile = "src/test/resources/big/centers"
  val centersGroupsFile = "src/test/resources/big/centersGroups"

  // --------------------------------------------------------------------------
  // Initialization of RDDs

  System.clearProperty("spark.driver.port")
  val sc = new SparkContext("local", "test")

  val graph = sc.textFile(graphFile).map(convertInput)

  val ballCardinalities = sc.textFile(ballsFile).map{line =>
    val data = line.split(" ")
    (data(0).toInt, data(1).toInt)
  }

  val colors = sc.textFile(colorsFile).map{line =>
    val data = line.split(" ")
    (data(0).toInt, data(1).toInt)
  }

  val centers = sc.textFile(centersFile).map(_.toInt)

  val centersGroups = sc.textFile(centersGroupsFile).map{line =>
    val data = line.split(" +")
    val nodeId = data.head.toInt
    val tData = data.tail
    var tuples: List[(Int,Int)] = List() // todo: use a mutable list
    for(i <- 1 until tuples.size/2 by 2) {
      tuples = tuples :+ (tData(i).toInt, tData(i+1).toInt)
    }
    (nodeId,tuples)
  }

  // --------------------------------------------------------------------------
  // Test teardown

  after {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  // --------------------------------------------------------------------------
  // Tests

  "Function computeBalls" should
    "compute balls of the correct cardinality" in {

    val computed = computeBalls(graph,1).map({
      case (nodeId, ball) => (nodeId, ball.size)
    }).collect.sorted

    ballCardinalities.collect().sorted.zip(computed).foreach {
      case (expected, actual) => assert( expected === actual )
    }

  }

}