package it.unipd.dei.graph.decompositions

import org.scalatest._
import BallDecomposition._
import spark.RDD
import scala.collection.mutable
import it.unipd.dei.graph._
import it.unipd.dei.graph.LocalSparkContext

class BigGraphBallDecompositionSpec extends FlatSpec with BeforeAndAfter
                                                     with LocalSparkContext {
  // --------------------------------------------------------------------------
  // Paths
  val graphFile = "src/test/resources/big/graph.adj"
  val ballsFile = "src/test/resources/big/balls_cardinalities"
  val colorsFile = "src/test/resources/big/colors"
  val centersFile = "src/test/resources/big/centers"
  val centersGroupsFile = "src/test/resources/big/centersGroups"

  var graph: RDD[(NodeId, Neighbourhood)] = _
  var ballCardinalities: RDD[(NodeId, Cardinality)] = _
  var colors: RDD[(NodeId, Color)] = _
  var centers: RDD[(NodeId)] = _
  var centersGroups: RDD[(NodeId, Seq[(Int,Int)])] = _

  // --------------------------------------------------------------------------
  // Initialization of RDDs

  before {
    graph = sc.textFile(graphFile).map(convertInput)

    ballCardinalities = sc.textFile(ballsFile).map{line =>
      val data = line.split(" ")
      (data(0).toInt, data(1).toInt)
    }

    colors = sc.textFile(colorsFile).map{line =>
      val data = line.split(" ")
      (data(0).toInt, data(1).toInt)
    }

    centers = sc.textFile(centersFile).map(_.toInt)

    centersGroups = sc.textFile(centersGroupsFile).map{line =>
      val data = line.split(" +")
      val nodeId = data.head.toInt
      val tData = data.tail
      var tuples: mutable.MutableList[(Int,Int)] = mutable.MutableList()
      for(i <- 0 to tData.size/2 by 2) {
        tuples += ( (tData(i).toInt, tData(i+1).toInt) )
      }
      (nodeId,tuples.toList)
    }
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
