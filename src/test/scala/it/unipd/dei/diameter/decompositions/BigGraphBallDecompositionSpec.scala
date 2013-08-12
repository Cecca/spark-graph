package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._
import spark.{RDD, SparkContext}
import SparkContext._
import scala.collection.mutable

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
    var tuples: mutable.MutableList[(Int,Int)] = mutable.MutableList()
    for(i <- 0 to tData.size/2 by 2) {
      tuples += ( (tData(i).toInt, tData(i+1).toInt) )
    }
    (nodeId,tuples.toList)
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

  "Function isCenter" should "tell if a node is a center" ignore {
    pending
  }

  it should "tell if a node is not a center" ignore {
    pending
  }

  "Function computeCenters" should "compute the correct centers" ignore {
    pending
  }

  "Function colorGraph" should "assign the correct colors to the graph" in {
    val balls = computeBalls(graph, 1)
    val actualColors = colorGraph(balls)

    colors.collect.sorted.zip(actualColors.collect.sorted).foreach { _ match {
        case (expected, actual) => assert( expected === actual )
      }
    }
  }

}