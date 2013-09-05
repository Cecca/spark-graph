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

import it.unipd.dei.graph._
import spark.SparkContext._
import spark.RDD

object RandomizedBallDecomposition extends Timed {

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def sendBalls(data: (NodeId, ((Boolean, Neighbourhood), Ball)))
  : TraversableOnce[(NodeId, Ball)] = data match {
    case (nodeId, ((isCenter, neigh), ball)) =>
      if(isCenter) neigh.map((_,ball)) :+ (nodeId, ball)
      else Seq()
  }

  def merge(ballA: Ball, ballB: Ball) =
    (ballA.distinct ++ ballB.distinct).distinct

  // --------------------------------------------------------------------------
  // Function on RDDs

  def computeBalls(graph: RDD[(NodeId,(Boolean,Neighbourhood))], radius: Int)
  : RDD[(NodeId, Ball)] = timed("Balls computation") {

    var balls = graph.map{case (nodeId, (_, neigh)) => (nodeId, neigh)}

    if ( radius == 1 ) {
      balls = balls.map({ case (nodeId, neigh) => (nodeId, neigh :+ nodeId) })
    } else {
      for(i <- 1 until radius) {
        val augmentedGraph = graph.join(balls)
        balls = augmentedGraph.flatMap(sendBalls).reduceByKey(merge)
      }
    }

    return balls
  }

  def randomizedBallDecomposition(graph: RDD[(NodeId, Neighbourhood)], radius: Int)
  : RDD[(NodeId, Neighbourhood)] = timed("Randomized ball decomposition") {

    // select randomly the ball centers

    // compute their balls

    // voting procedure

    // relalbe arcs

    null
  }

}
