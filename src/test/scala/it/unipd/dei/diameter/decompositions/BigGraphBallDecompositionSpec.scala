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
    val cents = centers.map((_,())).join(centersGroups).map {
      case (nodeId, (_, tuples)) => (nodeId, tuples)
    }.collect

//    cents.foreach{println(_)}

    cents.foreach { case (nodeId, tuples) =>
      if (tuples.size != 0) {
        val m = tuples.reduceLeft(max(_,_))
        assert( isCenter(nodeId, m) , "Fail on " + (nodeId, m) )
      } else {
        assert( false, "Empty tuple list" )
      }
    }
  }

  it should "tell if a node is not a center" ignore {
    val cents = centers.map((_,())).join(centersGroups).map {
      case (nodeId, (_, tuples)) => (nodeId, tuples)
    }
    val nonCents = centersGroups.subtractByKey(cents).collect

    nonCents.foreach { case (nodeId, tuples) =>
      val m = tuples.reduceLeft(max(_,_))
      assert( ! isCenter(nodeId, m) , "Fail on " + (nodeId, m) )
    }
  }

  "Function computeCenters" should "compute the correct centers" ignore {
    val computed = computeCenters(computeBalls(graph,1)).map {
      case (nodeId,_) => nodeId
    }.collect.sorted

    centers.collect.sorted.zip(computed).foreach {
      case (expected, actual) => assert( expected === actual )
    }

  }

}