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

}
