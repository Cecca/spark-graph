package it.unipd.dei.decompositions

import org.scalatest.FlatSpec
import it.unipd.dei.decompositions.BallDecomposition._
import it.unipd.dei.LocalSparkContext

class BallDecompositionSpecRDD extends FlatSpec with LocalSparkContext {

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
