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

  "Function convertAdj" should "correctly convert well formed input" in {
    info("with more than one neighbour")
    val neighsResult = convertInput("0 1 2 3 4 5")
    assert( (neighsResult._1, neighsResult._2.toSeq) === (0, Seq(1,2,3,4,5)))

    info("with no neighbours")
    val noNeighs = convertInput("0")
    assert( (noNeighs._1, noNeighs._2.toSeq) === (0, Seq()) )
  }

  "Function sendBalls" should "send balls to all neighbours" in {
    val ball = Array(1,2,3)
    info("with no neighbours")
    assert( sendBalls((0,(Array(),ball))) === Array((0,ball)) )
    info("with one neighbour")
    assert( sendBalls((0,(Array(1),ball))) === Array((1,ball),(0,ball)) )
    info("with two neighbours")
    assert( sendBalls((0,(Array(1,2),ball))) === Array((1,ball),(2,ball),(0,ball)) )
  }

  "Function merge" should "merge non empty balls" in {
    assert( merge(Array(1,2,3), Array(4,5,6)) === Array(1,2,3,4,5,6) )
  }

  it should "handle gracefully empty balls" in {
    assert( merge(Array(), Array()) === Array() )
    assert( merge(Array(1), Array()) === Array(1) )
    assert( merge(Array(), Array(1)) === Array(1) )
  }

  it should "handle overlapping Arrayuences by elminating duplicates" in {
    assert( merge(Array(1,2,3,4), Array(3,4,5)) === Array(1,2,3,4,5) )
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


