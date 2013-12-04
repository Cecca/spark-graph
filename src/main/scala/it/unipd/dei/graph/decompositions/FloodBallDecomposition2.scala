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

class FloodBallDecompositionVertex(
                                    val neighbours: Neighbourhood,
                                    val colors: Array[Color]) extends Serializable {

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
  private var _updateList: Array[Color] = colors

  /**
   * Gets the update list of this vertex and resets it.
   * @return the update list of this vertex
   */
  def updateList: Array[Color] = {
    val toRet = _updateList
    _updateList = Array()
    toRet
  }

  def isCenter(id: NodeId) = {
    colors.contains(id)
  }

  def merge(other: FloodBallDecompositionVertex): FloodBallDecompositionVertex = {
    val newColors = ArrayUtils.merge(this.colors, other.colors)
    val newUpdateList = ArrayUtils.merge(this._updateList, other._updateList)
    val mergedVertex = new FloodBallDecompositionVertex(neighbours, newColors)
    mergedVertex._updateList = newUpdateList
    mergedVertex
  }

  def addColors(cs: Array[Color]): FloodBallDecompositionVertex = {
    val newVertex = new FloodBallDecompositionVertex(neighbours, ArrayUtils.merge(colors, cs))
    newVertex._updateList = ArrayUtils.diff(cs, colors)
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
    new FloodBallDecompositionVertex(neighbours, newColors)
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
      
      val randomCentersColors = propagateColors(partitionedGraph, randomCenters, radius).forceAndDebug("First propagate colors")
      
      val missingCenters = selectMissingCenters(randomCentersColors).forceAndDebug("Missing centers select")

      val missingCentersColors = propagateColors(partitionedGraph, missingCenters, radius).forceAndDebug("Second propagate colors")

      val centers1 = propagateColors(partitionedGraph, randomCentersColors, radius + 1)
        .filter({case (id, vertex) => vertex.isCenter(id)})
        .mapValues({case center => center.colors})
        .forceAndDebug("Final propagation of random centers")

      val centers2 = propagateColors(partitionedGraph, missingCentersColors, radius + 1)
        .filter({case (id, vertex) => vertex.isCenter(id)})
        .mapValues({case center => center.colors})
        .forceAndDebug("Final propagation of missing centers")

      centers1.union(centers2)
        .forceAndDebug("Union")
    }
  }

  def partition(graph: RDD[(NodeId, Neighbourhood)]) : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    val numPartitions = graph.sparkContext.defaultParallelism

    logger.info("Partitioning graph in {} partitions, using HashPartitioner", numPartitions)

    graph
      .partitionBy(new HashPartitioner(numPartitions))
      .mapValues(neighs => new FloodBallDecompositionVertex(neighs.sorted, Array()))
      .force()
  }

  def selectCenters(graph: RDD[(NodeId, FloodBallDecompositionVertex)], centerProbability: Double)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {
    val centers: RDD[(NodeId, FloodBallDecompositionVertex)] =
      graph.flatMap({
        case (id, vertex) =>
          if (new Random().nextDouble() < centerProbability) {
            Seq((id, vertex.withNewColors(Array(id))))
          }
          else {
            Seq()
          }
      })

    centers
  }

  def selectMissingCenters(centers: RDD[(NodeId, FloodBallDecompositionVertex)])
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {
    val missing: RDD[(NodeId, FloodBallDecompositionVertex)] =
      centers.flatMap { case (node, vertex) =>
        if (vertex.colors.isEmpty) {
          Seq((node, vertex.withNewColors(Array(node))))
        }
        else {
          Seq()
        }
      }
    missing
  }

  def propagateColors(
                       graph: RDD[(NodeId, FloodBallDecompositionVertex)],
                       centers: RDD[(NodeId, FloodBallDecompositionVertex)],
                       radius: Int)
  : RDD[(NodeId, FloodBallDecompositionVertex)] = {

    logger.info("Propagating colors")

    val partitioner = centers.partitioner.getOrElse(new HashPartitioner(centers.sparkContext.defaultParallelism))

    var cnts = centers
    for(i <- 0 until radius) {
      val newColors = cnts
        .flatMap(sendColorsToNeighbours)
        .reduceByKey(partitioner, {(a, b) => merge(a,b)})
        .forceAndDebugCount("New colors")

//      cnts = cnts
//        .leftOuterJoin(newColors, partitioner)
//        .mapValues({
//          case (vertex, cs) => vertex.addColors(cs)
//        })
//        .forceAndDebug(" - Iteration " + i)

      cnts = graph.join(newColors)
        .mapValues({
          case (vertex, cs) => vertex.addColors(cs)
        })
        .join(cnts)
        .mapValues({case (u,v) => u merge v})
        .forceAndDebug(" - Iteration " + i)
    }

    cnts
  }

}
