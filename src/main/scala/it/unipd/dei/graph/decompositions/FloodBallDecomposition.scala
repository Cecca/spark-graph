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
import scala.Array
import org.apache.spark.HashPartitioner

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

  def forceEval(rdd: RDD[_]) = rdd.foreach(x => ())

  def floodBallDecomposition( graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = {

    val partitionedGraph = partition(graph)

    timedForce("flood-ball-decomposition") {

      logger.info("Selecting centers at random")
      val centers = selectCenters(partitionedGraph, centerProbability)

      logger.info("Coloring randomly selected centers ball neighbours")
      val coloredGraph = propagateColors(centers, radius)

      logger.info("Selecting uncolored nodes as centers")
      val missingCenters = selectMissingCenters(coloredGraph)

      logger.info("Coloring neighbours of newly selected nodes")
      val missingColors = propagateColors(missingCenters, radius)

      logger.info("Performing union of the two datasets")
      val finalColoredGraph = coloredGraph.union(missingColors)

      logger.info("Extracting colors")
      val colors = extractColors(finalColoredGraph).reduceByKey(merge)

      // shrink graph
      shrinkGraph(partitionedGraph, colors)
    }
  }

  def partition(graph: RDD[(NodeId, Neighbourhood)]) : RDD[(NodeId, Neighbourhood)] = {

    val numPartitions = graph.sparkContext.defaultParallelism

    logger.info("Partitioning graph in {} partitions, using HashPartitioner", numPartitions)

    graph.partitionBy(
      new HashPartitioner(numPartitions))
  }

  def selectCenters(graph: RDD[(NodeId, Neighbourhood)], centerProbability: Double)
  : RDD[(NodeId, (Neighbourhood, ColorList))] = {
    val numCenters = graph.sparkContext.accumulator[Long](0)
    val centers: RDD[(NodeId, (Neighbourhood, ColorList))] =
      graph.map({
        case (node, neighs) =>
          val isCenter = new Random().nextDouble() < centerProbability
          if (isCenter) {
            numCenters.add(1)
            (node, (neighs, Array(node)))
          }
          else
            (node, (neighs, Array()))
      })

    if(logger.isDebugEnabled) {
      forceEval(centers)
      logger.debug("There are {} centers", numCenters.value)
    }
    centers
  }

  def selectMissingCenters(centers: RDD[(NodeId, (Neighbourhood, ColorList))])
  : RDD[(NodeId, (Neighbourhood, ColorList))] = {
    val cnt = centers.sparkContext.accumulator(0L)
    val missing: RDD[(NodeId, (Neighbourhood, ColorList))] =
      centers.map { case (node, (neighs, colors)) =>
        if (colors.isEmpty) {
          cnt.add(1)
          (node, (neighs, Array(node)))
        }
        else
          (node, (neighs, Array()))
      }
    if(logger.isDebugEnabled()) {
      forceEval(missing)
      logger.debug("There are {} uncolored nodes", cnt.value)
    }
    missing
  }

  def propagateColors(centers: RDD[(NodeId, (Neighbourhood, ColorList))], radius: Int)
  : RDD[(NodeId, (Neighbourhood, ColorList))] = {
    var cnts = centers
    for(i <- 0 until radius) {
      val newColors = cnts.flatMap(sendColorsToNeighbours).reduceByKey(merge)
      val centCnt = newColors.count()
      logger.info("Iteration {}: colored {} nodes", i, centCnt)
      val grouped = cnts.leftOuterJoin(newColors)
      cnts = grouped.map(mergeColors)
    }

    if(logger.isDebugEnabled()) {
      val coloredNodes = cnts.filter{case (_, (_,cs)) => cs.nonEmpty}.count()
      logger.debug("There are {} colored nodes", coloredNodes)
    }
    cnts
  }

  def extractColors(centers: RDD[(NodeId, (Neighbourhood, ColorList))])
  : RDD[(NodeId, ColorList)] = {
    centers.map{case (node, (_, colors)) => (node, colors)}
  }

  def shrinkGraph(graph: RDD[(NodeId, Neighbourhood)], colors: RDD[(NodeId, ColorList)])
  : RDD[(NodeId, Neighbourhood)] = {
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
    transpose(colored)
  }

}
