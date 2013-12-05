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
import GraphForceFunctions._
import Timer._
import scala.collection.mutable

class FloodBallDecompositionVertex(
                                    val neighbours: Neighbourhood,
                                    /** The array of colors at distance <= k-1 at iteration k */
                                    val colors: Array[Color],
                                    /** The array of colors at distance exactly k at iteration k */
                                    val overlapZoneColors: Array[Color]) extends Serializable {

  /**
   * This is the list of updates to be exchanged with neighbours.
   * It is updated in the following occasions:
   *
   *  - The vertex is created: the update list consists of all the colors
   *    assigned to the node at the time of creation
   *  - New colors are added: the update list will have all the colors added
   *  - The update list is used: then it should be reset
   *  - Two vertices are merged: the update lists are merged too
   */
  private var _updateList: Array[Color] = overlapZoneColors

  /**
   * Gets the update list of this vertex and resets it.
   * @return the update list of this vertex
   */
  def updateList: Array[Color] = {
    val toRet = _updateList
    _updateList = Array()
    toRet
  }

  def allColors: Array[Color] = ArrayUtils.merge(overlapZoneColors, colors)

  def isCenter(id: NodeId): Boolean = {
    overlapZoneColors.contains(id)
  }

  def isCovered: Boolean = {
    !colors.isEmpty
  }

  /**
   * Returns true if the vertex should become a randomly selected center. The probability of a
   * positive outcome is given by
   *
   * {{{
   *      d_i
   *    -------
   *     delta
   * }}}
   *
   * where d_i is the outdegree of the node. If
   *
   * {{{
   *             2 m
   *    delta = ------
   *             n p
   * }}}
   *
   * with m the number of edges in the graph and n the number of nodes in the graph,
   * then the selected fraction of vertices will be `p n`.
   *
   * @param delta
   * @return
   */
  def selectVertex(delta: Double): Boolean = {
    new Random().nextDouble() <= (neighbours.length / delta)
  }

  def merge(other: FloodBallDecompositionVertex): FloodBallDecompositionVertex = {
    val newColors = ArrayUtils.merge(this.colors, other.colors)
    val newOverlapColors = ArrayUtils.merge(this.overlapZoneColors, other.overlapZoneColors)
    val newUpdateList = ArrayUtils.merge(this._updateList, other._updateList)
    val mergedVertex = new FloodBallDecompositionVertex(neighbours, newColors, newOverlapColors)
    mergedVertex._updateList = newUpdateList
    mergedVertex
  }

  def addColors(cs: Array[Color]): FloodBallDecompositionVertex = {
    val newVertex = new FloodBallDecompositionVertex(
      neighbours,
      cs,
      ArrayUtils.merge(overlapZoneColors, colors))
    newVertex._updateList = ArrayUtils.diff(cs, overlapZoneColors)
    newVertex
  }

  def addColors(maybeColors: Option[Array[Color]]): FloodBallDecompositionVertex = {
    maybeColors.map { cs =>
      addColors(cs)
    } getOrElse {
      this._updateList = Array() // the colors have already been sent in the previous iteration
      this
    }
  }

  def withNewColors(newColors: Array[Color]): FloodBallDecompositionVertex = {
    new FloodBallDecompositionVertex(neighbours, Array(), newColors)
  }
}

object FloodBallDecomposition2 {

  private val logger = LoggerFactory.getLogger(
    "algorithm.FloodBallDecomposition")

  // --------------------------------------------------------------------------
  // Map and reduce functions

  def sendColorsToNeighbours(data: (NodeId, FloodBallDecompositionVertex))
  : TraversableOnce[(NodeId, Array[Color])] = data match {
    case (id, vertex) => {
      val colors = vertex.updateList
      if(colors.nonEmpty)
        vertex.neighbours.map((_, colors))
      else
        Seq()
    }
  }

  def merge(a: Array[Color], b: Array[Color]): Array[Color] = {
    ArrayUtils.merge(a,b)
  }

  /**
   *
   * @param graph the input graph
   * @param p the fraction of desired centers
   * @return a tuple (num_nodes, num_edges, delta)
   */
  def computeDelta(
                    graph: RDD[(NodeId, FloodBallDecompositionVertex)],
                    p: Double)
  : (Long, Long, Double) = {
    val twiceNumEdgesAcc = graph.sparkContext.accumulator(0L)

    graph.foreach({case (_, vertex) => twiceNumEdgesAcc.add(vertex.neighbours.length) })
    val numNodes = graph.count()
    val twiceNumEdges = twiceNumEdgesAcc.value

    val delta = twiceNumEdges / (numNodes * p)
    
    (numNodes,(twiceNumEdges / 2), delta)
  }

  def floodBallDecomposition(
                              graph: RDD[(NodeId, Neighbourhood)],
                              radius: Int,
                              centerProbability: Double)
  : RDD[(NodeId, Neighbourhood)] = {

    if (radius == 0) {
      println("flood-ball-decomposition 0") // this line is for log mining with the python script
      logger.info("Ball decomposition of radius 0 is the graph itself!")
      return graph
    }

    val partitionedGraph = partition(graph)
    val (numNodes, numEdges, delta) = computeDelta(partitionedGraph, centerProbability)
    logger.info("Graph with {} nodes, {} edges. Delta = {}", numNodes+"", numEdges+"", delta+"")

    timedForce("flood-ball-decomposition") {

      val randomCentersColors = expandRandomBalls(partitionedGraph, delta, radius)

      val coloredGraph = expandMissingBalls(partitionedGraph, randomCentersColors, radius, 0.5)

      shrinkGraph(coloredGraph).forceAndDebug("  Graph shrinking")
    }
  }


  def expandRandomBalls(
                         partitionedGraph: RDD[(NodeId, FloodBallDecompositionVertex)],
                         delta: Double,
                         radius: Int)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("### Expanding randomly selected balls")

    val randomCenters = selectCenters(partitionedGraph, delta)
      .forceAndDebugCount("  Random centers select")

    propagateColors(partitionedGraph, randomCenters, radius + 1)
      .forceAndDebug("  Propagate colors for random balls")
  }

  def expandMissingBalls(
                          partitionedGraph: RDD[(NodeId, FloodBallDecompositionVertex)],
                          existingBalls: RDD[(NodeId, FloodBallDecompositionVertex)],
                          radius: Int,
                          probability: Double)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("### Expanding balls centered on uncovered nodes, with base probability {}", probability)

    var i = 1
    var merged = existingBalls
    var missingCenters = selectMissingCenters(partitionedGraph, merged, probability)
      .forceAndDebugCount("  Missing centers select")
    var missingCentersCount: Long = merged.filter(!_._2.isCovered).count()

    while (missingCentersCount > 0) {
      val prob = probability * i
      logger.info("  Iteration {}, probability of selection {}", i, prob)

      val missingCentersColors = propagateColors(partitionedGraph, missingCenters, radius + 1)
        .forceAndDebug("  Second propagate colors")

      merged = merged.union(missingCentersColors)
        .reduceByKey({ (u, v) => u merge v })
        .forceAndDebug("  Merge of graphs")

      missingCenters = selectMissingCenters(partitionedGraph, merged, prob)
        .forceAndDebugCount("  Missing centers select")

      missingCentersCount = merged.filter(!_._2.isCovered).count()
      logger.info("  Missing centers: {}", missingCentersCount)

      i += 1
    }

    merged
  }

  def partition(graph: RDD[(NodeId, Neighbourhood)]) : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    val numPartitions = graph.sparkContext.defaultParallelism

    logger.info("### Partitioning graph in {} partitions, using HashPartitioner", numPartitions)

    graph
      .partitionBy(new HashPartitioner(numPartitions))
      .mapValues(neighs => new FloodBallDecompositionVertex(neighs.sorted, Array(), Array()))
      .force()
  }

  def selectCenters(graph: RDD[(NodeId, FloodBallDecompositionVertex)], delta: Double)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("#### Selecting centers at random")

    graph.flatMap({
      case (id, vertex) =>
        if (vertex.selectVertex(delta)) {
          Seq((id, vertex.withNewColors(Array(id))))
        }
        else {
          Seq()
        }
    })

  }

  def selectMissingCenters(
                            graph: RDD[(NodeId, FloodBallDecompositionVertex)],
                            centers: RDD[(NodeId, FloodBallDecompositionVertex)],
                            probability: Double)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("#### Selecting ceneters from uncovered nodes")

    graph.union(centers)
      .reduceByKey({_ merge _})
      .flatMap { case (node, vertex) =>
        if(!vertex.isCovered && new Random().nextDouble() < probability) {
          Seq((node, vertex.withNewColors(Array(node))))
        } else {
          Seq()
        }
      }
  }

  def propagateColors(
                       graph: RDD[(NodeId, FloodBallDecompositionVertex)],
                       centers: RDD[(NodeId, FloodBallDecompositionVertex)],
                       radius: Int)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("#### Propagating colors")

    val partitioner = graph.partitioner.getOrElse(new HashPartitioner(centers.sparkContext.defaultParallelism))

    var cnts = centers
    for(i <- 0 until radius) {
      val newColors = cnts
        .flatMap(sendColorsToNeighbours)
        .reduceByKey(partitioner, {(a, b) => merge(a,b)}) // this is the costly operation
        .forceAndDebugCount("  New colors")

      cnts = graph
        .join(newColors, partitioner)
        .mapValues({case (vertex, cs) => vertex.addColors(cs)})
        .union(cnts)
        .reduceByKey(partitioner, {(u,v) => u merge v})
        .forceAndDebug(" - Iteration " + i)
    }

    cnts
  }

  def shrinkGraph(coloredNodes: RDD[(NodeId, FloodBallDecompositionVertex)])
  : RDD[(NodeId, Neighbourhood)] = {
    logger.info("### Sending colors to predecessors graph")

    coloredNodes
      .flatMap({ case (node, vertex) =>
        val cs = vertex.colors
        vertex.allColors.map({c =>  (c, cs)})
      })
      .reduceByKey({ArrayUtils.merge(_, _)})
      .filter({case (id, cs) => cs.contains(id)})
  }

}
