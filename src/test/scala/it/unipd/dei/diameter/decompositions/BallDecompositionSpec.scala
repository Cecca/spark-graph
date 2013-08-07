package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._

class BallDecompositionSpec extends FlatSpec {

  "Map function convert input" should "convert input" in {

    assert( convertInput("0 1 2 3 4") === (0, Seq(1, 2, 3, 4)) )
    assert( convertInput("0") === (0, Seq()) )
    assert( convertInput("0 3") === (0, Seq(3)) )

  }

  "Function sendBall" should "send balls to all neighbours and the node" in {

    val ball = Seq(3,4,6,5,2,8,9,1)
    val data = (0, (Seq(3,4,6,1), ball))

    val expected = List(
      (3, ball),
      (4, ball),
      (6, ball),
      (1, ball),
      (0, ball)
    )

    assert( sendBall(data) === expected )

  }

}
