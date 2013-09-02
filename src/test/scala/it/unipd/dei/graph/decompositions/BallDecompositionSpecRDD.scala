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

import org.scalatest.FlatSpec
import it.unipd.dei.graph.decompositions.BallDecomposition._
import it.unipd.dei.graph.LocalSparkContext

class BallDecompositionSpecRDD extends FlatSpec with LocalSparkContext {

  "Function computeBalls" should "correctly compute balls of radius one" in {
    val graph = sc.parallelize(Seq(
      (0,Array(1,2,3)),
      (1,Array(0)),
      (2,Array(0)),
      (3,Array(0))
    ))

    val balls = computeBalls(graph, 1).collect().map{case (n, neighs) => (n, neighs.toSeq)}
    val expected = Array(
      (0,Seq(1,2,3,0)),
      (1,Seq(0,1)),
      (2,Seq(0,2)),
      (3,Seq(0,3))
    )

    assert( balls === expected )
  }

}
