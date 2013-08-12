package it.unipd.dei.diameter.decompositions

import org.scalatest.{BeforeAndAfter, OneInstancePerTest, FlatSpec}
import spark.SparkContext
import it.unipd.dei.diameter.decompositions.BallDecomposition._

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
