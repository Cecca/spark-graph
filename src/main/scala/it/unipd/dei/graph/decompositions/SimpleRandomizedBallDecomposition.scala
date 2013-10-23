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

import org.slf4j.LoggerFactory
import it.unipd.dei.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.Left
import org.apache.spark.broadcast.Broadcast
import scala.util.Random

object SimpleRandomizedBallDecomposition extends BallComputer
                                            with ArcRelabeler
                                            with Timed {

  private val logger = LoggerFactory.getLogger(
    "algorithm.SimpleRandomizedBallDecomposition")

  object UncoloredNodeStatus {
    val Center : Byte = 0
    val NotCenter : Byte = 1
  }
  type UncoloredNodeStatus = Byte
  import UncoloredNodeStatus._

  type NodeTag = (Either[UncoloredNodeStatus, Color], Ball)

  type TaggedGraph = RDD[(NodeId, NodeTag)]

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def colorDominated(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, (Color, Cardinality))] = data match {
    case (node, (Left(Center), ball)) => {
      val card = ball.size
      ball map { (_,(node, card)) }
    }
    case _ => Seq()
  }

  def applyColors(data: (NodeId, (NodeTag, Option[(Color,Cardinality)])))
  : (NodeId, Color) = data match {
    case (node, ((Right(color), ball), _)) => (node, color)
    case (node, ((Left(Center), ball), _)) => (node, node)
    case (node, ((Left(NotCenter), ball), newColor)) =>
      newColor map { case (color, _) =>
        (node, color)
      } getOrElse {
        (node, node)
      }
  }

  // --------------------------------------------------------------------------
  // Function on RDDs

  def colorGraph(taggedGraph: TaggedGraph)
  : RDD[(NodeId, Color)] = {

    val newColors = taggedGraph.flatMap(colorDominated).reduceByKey(max)
    taggedGraph.leftOuterJoin(newColors)
               .map(applyColors)
  }

  def simpleRandomizedBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                                   radius: Int,
                                   centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = timed("Simple randomized ball decomposition") {

    // compute balls
    val balls = computeBalls(graph, radius)

    // select node centers
    val taggedGraph: TaggedGraph = balls.map { case (node, ball) =>
      val size = ball.size
      val score = (new Random().nextDouble() * size) / size
      if(score < centerProbability.value)
        (node, (Left(Center), ball))
      else
        (node, (Left(NotCenter), ball))
    }

    // mark all neighbours, without voting
    val colors = colorGraph(taggedGraph)

    val relabeled = relabelArcs(graph, colors)

    // force evaluation by printing
    val numNodes = relabeled.count()
    logger info ("Quotient cardinality: {}", numNodes)

    relabeled
  }

}
