package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._

class BallDecompositionSpec extends FlatSpec {

  "Function convertInput" should "convert input" in {

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

  "Function reduceBalls" should "return the merge of the balls without duplicates" in {

    assert( reduceBalls(Seq(1,2,3), Seq(5,6,7)) === Seq(1,2,3,5,6,7) )
    assert( reduceBalls(Seq(1,2,3), Seq(1,2,5,6,7)) === Seq(1,2,3,5,6,7) )
    assert( reduceBalls(Seq(1,2,2,3), Seq(5,6,7)) === Seq(1,2,3,5,6,7) )

  }

  "Function removeSelfLoops" should "remove th ID from the ball" in {

    assert( removeSelfLoops(0, Seq(0,1,2,3,4)) === (0, Seq(1,2,3,4)) )
    assert( removeSelfLoops(0, Seq(1,2,3,4)) === (0, Seq(1,2,3,4)) )
    assert( removeSelfLoops(0, Seq(0,1,2,3,0,4)) === (0, Seq(1,2,3,4)) )

  }

  "Function countCardinalities" should "tell the size of a ball" in {

    assert( countCardinalities((0,Seq(1,2,3,4,5,6))) === (0,(6,Seq(1,2,3,4,5,6))) )
    assert( countCardinalities((0,Seq())) === (0,(0,Seq())) )

  }

  "Function sendCardinalities" should
    "send cardinalities to all neighbours in the ball" in {

    assert(
      sendCardinalities(0, (6,Seq(1,2,3,4,5,6))) ===
      List(
        (1,(0,6)), (2,(0,6)), (3,(0,6)), (4,(0,6)),
        (5,(0,6)), (6,(0,6)), (0,(0,6))
      )
    )

  }

  "Function maxCardinality" should "find the maximum cardinality" in {

    assert( maxCardinality((0,2), (1,4)) === (1,4) )
    assert( maxCardinality((5,35), (1,4)) === (5,35) )
    assert( maxCardinality((1,4), (1,4)) === (1,4) )

  }

  it should "order by ID in case of equal cadinality" in {

    assert( maxCardinality((0,2), (1,2)) === (1,2) )

  }

  "Function removeCardinality" should "remove the cardinality from the pair" in {

    assert( removeCardinality((0,(1,2))) == (0,1) )

  }

  "Function sortPair" should "return a new pair with sorted elements" in {

    assert( sortPair((1,2)) === (1,2) )
    assert( sortPair((2,1)) === (1,2) )
    assert( sortPair((1,1)) === (1,1) )

  }

}
