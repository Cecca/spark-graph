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

import spark.RDD
import spark.SparkContext._
import it.unipd.dei.graph._

/**
 * This trait adds the capability to relabel arcs.
 *
 * An arc is relabeled by changing the IDs of its endpoints with their
 * respective colors.
 */
trait ArcRelabeler {
  def relabelArcs(graph: RDD[(NodeId,Neighbourhood)], colors: RDD[(NodeId, Color)])
  : RDD[(NodeId,Neighbourhood)] = {

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
}
