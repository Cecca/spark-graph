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

import scala.Some
import it.unipd.dei.graph._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.util.Random
import org.slf4j.LoggerFactory
import scala.Array
import org.apache.spark.HashPartitioner
import GraphForceFunctions._
import Timer._

object FloodBallDecomposition2 {

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

  def sendColorsToCenters(data: (NodeId, (ColorList)))
  : TraversableOnce[(NodeId, ColorList)] = data match {
    case (node, (cList)) => cList.map((_, cList))
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
    (a ++ b).distinct
  }

  def createColorCombiner(data: (Neighbourhood, ColorList)): ColorList = data._2
  def unionColorCombiners(a: ColorList, b: ColorList): ColorList = (a ++ b).distinct
  def mergeColorCombiners(cList: ColorList, value: (Neighbourhood, ColorList)): ColorList = (cList ++ value._2).distinct

  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = {

    if (radius == 0) {
      println("flood-ball-decomposition 0") // this line is for log mining with the python script
      logger.info("Ball decomposition of radius 0 is the graph itself!")
      return graph
    }

    val partitionedGraph = partition(graph)

    timedForce("flood-ball-decomposition") {
      val randomCenters = selectCenters(partitionedGraph, centerProbability).forceAndDebug("Random centers select")
      
      val randomCentersColors = propagateColors(randomCenters, radius).forceAndDebug("First propagate colors")
      
      val missingCenters = selectMissingCenters(randomCentersColors).forceAndDebug("Missing centers select")

      val missingCentersColors = propagateColors(missingCenters, radius).forceAndDebug("Second propagate colors")

      val coloredGraph = randomCentersColors.union(missingCentersColors)
        .reduceByKey({(a,b) => (a._1, (a._2 ++ b._2).distinct)})
        .forceAndDebug("Union")

      propagateColors(coloredGraph, radius + 1)
        .filter({case (node, (_, cs)) => cs.contains(node)})
        .mapValues({case (_, cs) => cs})
        .forceAndDebug("Final propagation")
    }
  }

  def partition(graph: RDD[(NodeId, Neighbourhood)]) : RDD[(NodeId, Neighbourhood)] = {

    val numPartitions = graph.sparkContext.defaultParallelism

    logger.info("Partitioning graph in {} partitions, using HashPartitioner", numPartitions)

    graph.partitionBy(new HashPartitioner(numPartitions)).force()
  }

  def selectCenters(graph: RDD[(NodeId, Neighbourhood)], centerProbability: Double)
  : RDD[(NodeId, (Neighbourhood, ColorList))] = {
    val centers: RDD[(NodeId, (Neighbourhood, ColorList))] =
      graph.map({
        case (node, neighs) =>
          val isCenter = new Random().nextDouble() < centerProbability
          if (isCenter) {
            (node, (neighs, Array(node)))
          }
          else
            (node, (neighs, Array()))
      })

    if(logger.isDebugEnabled) {
      logger.debug("There are {} centers", centers.filter({case (n, (_,cs)) => cs.contains(n)}).count())
    }
    centers
  }

  def selectMissingCenters(centers: RDD[(NodeId, (Neighbourhood, ColorList))])
  : RDD[(NodeId, (Neighbourhood, ColorList))] = {
    val missing: RDD[(NodeId, (Neighbourhood, ColorList))] =
      centers.map { case (node, (neighs, colors)) =>
        if (colors.isEmpty) {
          (node, (neighs, Array(node)))
        }
        else
          (node, (neighs, Array()))
      }
    if(logger.isDebugEnabled()) {
      logger.debug("There are {} uncolored nodes", missing.filter({case (n, (_, cs)) => cs.contains(n)}).count())
    }
    missing
  }

  def propagateColors(centers: RDD[(NodeId, (Neighbourhood, ColorList))], radius: Int)
  : RDD[(NodeId, (Neighbourhood,ColorList))] = {
    var cnts = centers
    for(i <- 0 until radius) {
      val newColors = cnts.flatMap(sendColorsToNeighbours).reduceByKey(merge)
      if(logger.isDebugEnabled) {
        val centCnt = newColors.count()
        logger.debug("Iteration {}: colored {} nodes", i, centCnt)
      }
      val grouped = cnts.leftOuterJoin(newColors)
      cnts = grouped.map(mergeColors).forceAndDebug(" - Iteration " + i)
    }

    if(logger.isDebugEnabled()) {
      val coloredNodes = cnts.filter{case (_, (_,cs)) => cs.nonEmpty}.count()
      logger.debug("There are {} colored nodes", coloredNodes)
    }
    cnts
  }

}
