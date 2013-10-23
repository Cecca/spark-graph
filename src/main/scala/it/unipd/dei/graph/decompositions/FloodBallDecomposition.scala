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
    "algorithm.FloodBallDecomposition")

  private type ColorList = Array[Color]

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def sendColors(data: (NodeId, (Neighbourhood, ColorList)))
  : TraversableOnce[(NodeId, ColorList)] = data match {
    case (node, (neighs, cList)) =>
      if(cList.nonEmpty)
        neighs.map((_, cList))
      else
        Seq()
  }

  def mergeColors(data: (NodeId, (Seq[(Neighbourhood, ColorList)], Seq[ColorList])))
  : (NodeId, (Neighbourhood, ColorList)) = data match {
    case (node, (vertex, colors)) if vertex.size == 1 => {
      vertex.map { case (neighs, oldColors) =>
        // TODO merge with more efficiency
        val newColors: ColorList =
          if(colors.nonEmpty)
            (colors.reduce(_ ++ _) ++ oldColors).distinct
          else
            oldColors
        (node, (neighs, newColors))
      }.head
    }
    case (node, (vertex, _)) =>
      throw new IllegalArgumentException("Id " + node + " has " + vertex.size + " associated vertices")
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = timed("flood-ball-decomposition") {

    // select centers at random
    logger.info("Selecting centers")
    var centers: RDD[(NodeId, (Neighbourhood, ColorList))] =
      graph.map({ case (node, neighs) =>
        val isCenter = new Random().nextDouble() < centerProbability
        if(isCenter)
          (node, (neighs, Array(node)))
        else
          (node, (neighs, Array()))
      })

    val numCenters =
      centers.filter{case (_, (_,c)) => c.nonEmpty}.count()
    logger.info("There are {} centers", numCenters)

    // propagate their colors
    logger.info("Propagating colors")
    for(i <- 0 until radius) {
      logger.info("Iteration {}", i)
      val newColors = centers.flatMap(sendColors)
      val grouped = centers.cogroup(newColors)
      centers = grouped.map(mergeColors)
    }

    val coloredNodes = centers.filter{case (_, (_,cs)) => cs.nonEmpty}.count()
    logger.info("There are {} colored nodes", coloredNodes)

    // assign color to nodes missing it and remove the boolean flag
    val colors: RDD[(NodeId, (Neighbourhood, ColorList))] =
      centers.map { case (node, (neighs, colors)) =>
        if(colors.isEmpty) {
          (node, (neighs, Array(node)))
        } else {
          (node, (neighs, colors))
        }
      }

    // create edges with all the colors and relabel them
    logger.info("Relabeling sources of edges")
    val coloredSources: RDD[(NodeId, ColorList)] =
      colors.flatMap { case (node, (neighs, colors)) =>
         neighs map { (_, colors) }
      }.reduceByKey { (a, b) =>
        (a ++ b).distinct
      }

    logger.info("Relabeling destinations of edges")
    val edges =
      colors.cogroup(coloredSources).flatMap { case (node, (vertex, sourcesColors)) =>
        vertex.head match {
          case (_, colors) => {
            val localEdges = for (
              sColors <- sourcesColors;
              s <- sColors;
              c <- colors
            ) yield (s, c)
            localEdges.distinct
          }
        }
      }

    // now revert to an adjacency list representation
    logger.info("Reverting to an adjacency list representation")
    val reduced =
      edges.groupByKey().map{case (node, neighs) => (node, neighs.distinct.toArray)}

    // force evaluation
    reduced.foreach(x => {})

    reduced

  }

}
