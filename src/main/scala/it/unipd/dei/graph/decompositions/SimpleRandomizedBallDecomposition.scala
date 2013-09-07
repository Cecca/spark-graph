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
import spark.RDD
import spark.SparkContext._
import scala.Left
import spark.broadcast.Broadcast
import scala.util.Random

object SimpleRandomizedBallDecomposition extends Timed {

  private val logger = LoggerFactory.getLogger(
    "SimpleRandomizedBallDecomposition")

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

  def sendBalls(data: (NodeId, (Neighbourhood, Ball))) = data match {
    case (nodeId, (neigh, ball)) =>
      neigh.map((_,ball)) :+ (nodeId, ball)
  }

  def merge(ballA: Ball, ballB: Ball) =
    (ballA.distinct ++ ballB.distinct).distinct

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

  def computeBalls(graph: RDD[(NodeId,Neighbourhood)], radius: Int)
  : RDD[(NodeId, Ball)] = timed("Balls computation") {

    var balls = graph.map(data => data) // simply copy the graph

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

  def colorGraph(taggedGraph: TaggedGraph)
  : RDD[(NodeId, Color)] = {

    val newColors = taggedGraph.flatMap(colorDominated).reduceByKey(max)
    taggedGraph.leftOuterJoin(newColors)
               .map(applyColors)
  }

  def relabelArcs(graph: RDD[(NodeId,Neighbourhood)], colors: RDD[(NodeId, Color)])
  : RDD[(NodeId,Neighbourhood)] = timed("Relabeling") {

    var edges: RDD[(NodeId,NodeId)] =
      graph.flatMap { case (src, neighs) => neighs map { (src,_) } }

    // replace sources with their color
    edges = edges.join(colors)
      .map{ case (src, (dst, srcColor)) => (dst, srcColor) }

    // replace destinations with their colors
    edges = edges.join(colors)
      .map{ case (dst, (srcColor, dstColor)) => (srcColor, dstColor) }

    // now revert to an adjacency list representation
    edges.groupByKey().map{case (node, neighs) => (node, neighs.distinct.toArray)}
  }

  def simpleRandomizedBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                                   radius: Int,
                                   centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = timed("Simple randomized ball decomposition") {

    // compute balls
    val balls = computeBalls(graph, radius)

    // select node centers
    val taggedGraph: TaggedGraph = balls.map { case (node, ball) =>
      if(new Random().nextDouble() < centerProbability.value)
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
