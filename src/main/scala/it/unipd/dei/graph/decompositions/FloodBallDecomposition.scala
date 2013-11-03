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

  def sendColorsToNeighbours(data: (NodeId, (Neighbourhood, ColorList)))
  : TraversableOnce[(NodeId, ColorList)] = data match {
    case (node, (neighs, cList)) =>
      if(cList.nonEmpty)
        neighs.map((_, cList))
      else
        Seq()
  }

  def sendColorsToCenters(data: (NodeId, (Neighbourhood, ColorList)))
  : TraversableOnce[(NodeId, ColorList)] = data match {
    case (node, (neighs, cList)) =>
      val dests = neighs.toSet.intersect(cList.toSet)
      dests.map((_, cList))
  }

  def mergeColors(data: (NodeId, ( (Neighbourhood, ColorList) , Option[ColorList] )))
  : (NodeId, (Neighbourhood, ColorList)) = data match {
    case (node, ( (neighs, oldColors), Some(newColors) )) => {
      (node, (neighs, merge(oldColors, newColors)))
    }
    case (node, ( vertexData, None)) => {
      (node, vertexData)
    }
  }

  def merge(a: Array[Color], b: Array[Color]): Array[Color] = {
    // TODO merge with more efficiency
    a.toSet.union(b.toSet).toArray
  }

  // --------------------------------------------------------------------------
  // Functions on RDDs

  def transpose( graph: RDD[(NodeId, Neighbourhood)] )
  : RDD[(NodeId, Neighbourhood)] = timedForce("transpose-graph", false) {
    graph.flatMap { case (node, neighs) => neighs.map((_, node)) }
      .groupByKey().map{case (node, inNeighs) => (node, inNeighs.distinct.toArray)}
  }


  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = timedForce("flood-ball-decomposition") {

    // select centers at random
    logger.info("Selecting centers")
    val numCenters = graph.sparkContext.accumulator[Long](0)
    var centers: RDD[(NodeId, (Neighbourhood, ColorList))] =
      graph.map({ case (node, neighs) =>
        val isCenter = new Random().nextDouble() < centerProbability
        if(isCenter) {
          numCenters.add(1)
          (node, (neighs, Array(node)))
        }
        else
          (node, (neighs, Array()))
      })
    centers.foreach(x => ())
    logger.info("There are {} centers", numCenters.value)

    // propagate their colors
    logger.info("Propagating colors")
    for(i <- 0 until radius) {
      val newColors = centers.flatMap(sendColorsToNeighbours).reduceByKey(merge)
      val centCnt = newColors.count()
      logger.info("Iteration {}: colored {} nodes", i, centCnt)
      val grouped = centers.leftOuterJoin(newColors)
      centers = grouped.map(mergeColors)
    }

    val coloredNodes = centers.filter{case (_, (_,cs)) => cs.nonEmpty}.count()
    logger.info("There are {} colored nodes", coloredNodes)

    // assign color to nodes missing it
    val nonColoredNodes = graph.sparkContext.accumulator[Long](0)
    val colors: RDD[(NodeId, ColorList)] = timedForce("assign-missing-colors", false) {
      centers.map { case (node, (neighs, colors)) =>
        if(colors.isEmpty) {
          nonColoredNodes.add(1)
          (node, Array(node))
        } else {
          (node, colors)
        }
      }
    }
    colors.foreach(x => ())
    logger.info("There were {} uncolored nodes that were colored with their own ID", nonColoredNodes.value)

    // shrink graph
    logger.info("Transposing original graph")
    val transposedGraph = transpose(graph)

    logger.info("Sending colors to predecessors in transposed graph")
    val colored = timedForce("sending-colors", false) {
      transposedGraph.join(colors)
        .flatMap(sendColorsToCenters)
        .reduceByKey{ (a, b) => (a ++ b).distinct }
        .filter{ case (n, cs) => cs.contains(n) }
    }

    logger.info("Transposing colored graph to get the quotient")
    val quotient = transpose(colored)

    quotient
  }

}
