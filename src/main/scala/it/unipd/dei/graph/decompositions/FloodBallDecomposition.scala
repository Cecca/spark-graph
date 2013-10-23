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
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.Random
import org.slf4j.LoggerFactory

object FloodBallDecomposition extends Timed {

  private val logger = LoggerFactory.getLogger(
    "FloodBallDecomposition")

  private type ColorList = Array[Color]

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def sendColors(data: (NodeId, (Boolean, Neighbourhood, ColorList)))
  : TraversableOnce[(NodeId, ColorList)] = data match {
    case (node, (true, neighs, cList)) => {
      neighs.map((_, cList))
    }
    case (_, (false, _, _)) => Seq()
  }

  def mergeColors(data: (NodeId, (Seq[(Boolean, Neighbourhood, ColorList)], Seq[ColorList])))
  : (NodeId, (Boolean, Neighbourhood, ColorList)) = data match {
    case (node, (vertex, colors)) if vertex.size == 1 => {
      vertex.map { case (center, neighs, oldColors) =>
        // TODO merge with more efficiency
        val newColors: ColorList = colors.fold(oldColors)((a, b) => (a.distinct ++ b.distinct).distinct)
        (node, (center, neighs, newColors))
      }.head
    }
    case (node, (vertex, _)) =>
      throw new IllegalArgumentException("Node " + node + " has " + vertex.size + " associated vertices")
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = timed("flood-ball-decomposition") {

    // select centers at random
    logger.info("Selecting centers")
    var centers: RDD[(NodeId, (Boolean, Neighbourhood, ColorList))] =
      graph.map({ case (node, neighs) =>
        val isCenter = new Random().nextDouble() < centerProbability
        (node, (isCenter, neighs, Array(node)))
      })

    // propagate their colors
    logger.info("Propagating colors")
    for(i <- 0 until radius) {
      logger.info("Iteration {}", i)
      val newColors = centers.flatMap(sendColors)
      val grouped = centers.cogroup(newColors)
      centers = grouped.map(mergeColors)
    }

    // create edges with all the colors and relabel them
    logger.info("Relabeling sources of edges")
    var edges: RDD[(NodeId, NodeId)] =
      centers.flatMap { case (node, (_, neighs, colors)) =>
        for ( c <- colors; n <- neighs ) yield (n, c)
      }

    logger.info("Relabeling destinations of edges")
    edges = centers.cogroup(edges).flatMap { case (node, (vertex, sources)) =>
      vertex.head match {
        case (_, _, colors) => {
          for ( s <- sources ; c <- colors ) yield (s, c)
        }
      }
    }

    // now revert to an adjacency list representation
    logger.info("Reverting to an adjacency list representation")
    edges.groupByKey().map{case (node, neighs) => (node, neighs.distinct.toArray)}

  }

}
