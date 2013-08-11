package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._
import spark.{RDD, SparkContext}
import SparkContext._

class BallDecompositionSpec extends FlatSpec {

  "Function convertInput" should "correctly convert well formed input" in {
    info("with more than one neighbour")
    assert( convertInput("0 1 2 3 4 5") === (0, Seq(1,2,3,4,5)))
    info("with no neighbours")
    assert( convertInput("0") === (0, Seq()) )
  }

  "Function sendBalls" should "send balls to all neighbours" in {
    val ball = Seq(1,2,3)
    info("with no neighbours")
    assert( sendBalls((0,(Seq(),ball))) === Seq((0,ball)) )
    info("with one neighbour")
    assert( sendBalls((0,(Seq(1),ball))) === Seq((1,ball),(0,ball)) )
    info("with two neighbours")
    assert( sendBalls((0,(Seq(1,2),ball))) === Seq((1,ball),(2,ball),(0,ball)) )
  }

  "Function merge" should "merge non empty balls" in {
    assert( merge(Seq(1,2,3), Seq(4,5,6)) === Seq(1,2,3,4,5,6) )
  }

  it should "handle gracefully empty balls" in {
    assert( merge(Seq(), Seq()) === Seq() )
    assert( merge(Seq(1), Seq()) === Seq(1) )
    assert( merge(Seq(), Seq(1)) === Seq(1) )
  }

  it should "handle overlapping sequences by elminating duplicates" in {
    assert( merge(Seq(1,2,3,4), Seq(3,4,5)) === Seq(1,2,3,4,5) )
  }

}

class BallDecompositionSpecRDD extends FlatSpec with OneInstancePerTest
                                                with BeforeAndAfter {

  System.clearProperty("spark.driver.port")
  val sc = new SparkContext("local", "test")

  after {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  "Function computeBalls" should "correctly compute balls of radius one" in {
    val graph = sc.parallelize(Seq(
      (0,Seq(1,2,3)),
      (1,Seq(0)),
      (2,Seq(0)),
      (3,Seq(0))
    ))

    val balls = computeBalls(graph, 1).collect()
    val expected = Array(
      (0,Seq(1,2,3,0)),
      (1,Seq(0,1)),
      (2,Seq(0,2)),
      (3,Seq(0,3))
    )

    assert( balls === expected )
  }

}
