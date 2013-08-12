package it.unipd.dei.diameter.decompositions

import org.scalatest._
import BallDecomposition._

class BallDecompositionSpec extends FlatSpec with GivenWhenThen {

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

  "Function sendCardinalities" should
    "send the ball cardinality to all ball neighbours" in {
    val ball = Seq(0,1,2,3,4)
    val expected = List(
      (0,(0,ball.size)),
      (1,(0,ball.size)),
      (2,(0,ball.size)),
      (3,(0,ball.size)),
      (4,(0,ball.size))
    )

    assert( expected === sendCardinalities((0,ball)) )
  }

  "Function max" should "find the maximum between two cardinalities" in {
    assert( max((0,2),(2,1)) === (0,2) )
    assert( max((0,2),(2,15)) === (2,15) )
    assert( max((-1,-1),(2,15)) === (2,15) )
  }

  it should "break ties using the node IDs" in {
    info(
      "The greater the ID, the greater the pair, in case of equal cardinalities")
    assert( max((0,2),(1,2)) === (1,2) )
    assert( max((4,2),(1,2)) === (4,2) )
  }

  "Function isCenter" should "tell if the node is a ball center" in {
    val cardinalities = Seq((0,5),(1,1),(2,1),(3,1),(4,1))
    val ball = Seq(0,1,2,3,4)

    assert( isCenter((0, (cardinalities, ball))) )
  }

  it should "tell if a node is not a ball center" in {
    val cardinalities = Seq((0,5),(1,1),(2,1),(3,6),(4,1))
    val ball = Seq(0,1,2,3,4)

    assert( ! isCenter((0, (cardinalities, ball))) )

  }

  "Function colorDominated" should "return all the dominated nodes, colored" in {
    val ball = Seq(0,1,2,3,4)
    val cardinalities = Seq((0,5),(1,1),(2,1),(3,1),(4,1))
    val expected = Seq(
      ((0,(0,5))),
      ((1,(0,5))),
      ((2,(0,5))),
      ((3,(0,5))),
      ((4,(0,5)))
    )

    val actual = colorDominated((0, (cardinalities, ball))).toSeq.sorted

    assert( actual === expected )
  }

}


