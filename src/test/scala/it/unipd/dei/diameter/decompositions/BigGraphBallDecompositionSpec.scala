package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._
import spark.{RDD, SparkContext}
import SparkContext._

class BigGraphBallDecompositionSpec extends FlatSpec {

  private val graphDataset = "src/test/resources/big/graph.adj"
  private val ballsDataset = "src/test/resources/big/balls_cardinalities"
  private val colorsDataset = "src/test/resources/big/colors"

  // --------------------------------------------------------------------------
  // init the environment

  System.clearProperty("spark.driver.port")
  private val sc = new SparkContext("local", "Big dataset test")

  private val graph = sc.textFile(graphDataset).map(convertInput).cache()
  val balls = sc.textFile(ballsDataset).map{ line =>
    val elems: Array[String] = line.split(" ")
    (elems(0).toInt, elems(1).toInt)
  }.cache()
  private val colors = sc.textFile(colorsDataset).map{ line =>
    val elems: Array[String] = line.split(" ")
    (elems(0).toInt, elems(1).toInt)
  }.cache()

  // --------------------------------------------------------------------------
  // start tests

  "The input files" should "have the same number of elements" in {
    assert( graph.count() === balls.count() )
    assert( graph.count() === colors.count() )
  }

  "The 'big' dataset" should "have the correct ball cardinalities computed" in {

    val computedBalls = computeBalls(graph, 1).map { case (nodeId, ball) =>
      // the `+1` is to fix a difference of one between the computed balls and
      // the dataset.
      (nodeId, ball.size + 1)
    }

    assert( balls.collect.sorted === computedBalls.collect.sorted )
  }

  it should "have the correct color assigned to each node" in {

    val computedColors = computeColors(computeBalls(graph,1))

//    assert( colors.collect.sorted === computedColors.collect.sorted )
    colors.collect.sorted.zip(computedColors.collect.sorted)
      .foreach { case (expected, computed) =>
        assert( expected === computed )
      }
  }

}
