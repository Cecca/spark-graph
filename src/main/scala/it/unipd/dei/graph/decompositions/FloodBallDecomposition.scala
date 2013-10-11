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
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.slf4j.LoggerFactory
import scala.Left
import org.apache.spark.broadcast.Broadcast

object FloodBallDecomposition extends ArcRelabeler with Timed {

  private val logger = LoggerFactory.getLogger(
    "FloodBallDecomposition")

  object UncoloredNodeStatus {
    val Center : Byte = 0
    val NonCenter : Byte = 1
  }
  type UncoloredNodeStatus = Byte
  import UncoloredNodeStatus._

  type NodeTag = (Either[UncoloredNodeStatus, Color], Neighbourhood)

  type TaggedGraph = RDD[(NodeId, NodeTag)]

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def sendColors(data: (NodeId, NodeTag))
  : TraversableOnce[(NodeId, (Color, Cardinality))] = data match {
    case (node, (Right(color), neighs)) => neighs map { (_,(color, neighs.size)) }
    case (node, (Left(Center), neighs)) => neighs map { (_,(node, neighs.size)) }
    case (_, (Left(NonCenter), _)) => Seq()
  }

  /**
   * Applies a color only if the node is not a center and is not already colored
   * If the node received no color, then it is left uncolored
   * @param data
   * @return
   */
  def applyColors(data: (NodeId, (NodeTag, Option[(Color, Cardinality)])))
  : (NodeId, NodeTag) = data match {
    case (node, ((origCol@Right(_), neighs), _)) => (node, (origCol, neighs))
    case (node, ((Left(Center), neighs), _)) => (node, (Left(Center), neighs))
    case (node, ((Left(NonCenter), neighs), Some((color,_)))) =>
      (node, (Right(color), neighs))
    case (node, ((Left(NonCenter), neighs), None)) =>
      (node, (Left(NonCenter), neighs))
  }

  def extractColor(data: (NodeId, NodeTag))
  : (NodeId, Color) = data match {
    case (node, (Right(color), _)) => (node, color)
    case (node, (Left(_), _)) => (node, node)
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def propagateColors(taggedGraph: TaggedGraph, radius: Int)
  : RDD[(NodeId, Color)] = {
    var tGraph = taggedGraph
    for(i <- 1 to radius) {
      val newColors = tGraph.flatMap(sendColors).reduceByKey(max)
      tGraph = tGraph.leftOuterJoin(newColors).map(applyColors)
    }

    tGraph.map(extractColor)
  }

  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Broadcast[Double])
  : RDD[(NodeId, Neighbourhood)] = {

    // mark centers
    val taggedGraph: TaggedGraph = graph.map { case (node, neighbourhood) =>
      if(new Random().nextDouble() < centerProbability.value)
        (node, (Left(Center), neighbourhood))
      else
        (node, (Left(NonCenter), neighbourhood))
    }

    // propagate colors until ball radius is hit.
    val colors = propagateColors(taggedGraph, radius)

    // relabel
    val relabeled = relabelArcs(graph, colors)

    // force evaluation by printing
    val numNodes = relabeled.count()
    logger info ("Quotient cardinality: {}", numNodes)

    relabeled
  }

}
