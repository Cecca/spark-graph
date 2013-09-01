/*
 * Copyright (C) 2013 Matteo Ceccarello
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package it.unipd.dei.graph.decompositions

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

}


