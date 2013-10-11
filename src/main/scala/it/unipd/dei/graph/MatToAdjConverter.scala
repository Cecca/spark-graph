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

package it.unipd.dei.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * Converts a dataset in sparse matrix form to a dataset in adjacency list form
 */
trait MatToAdjConverter {

  def matToAdj(in: RDD[(NodeId, NodeId)]): RDD[(NodeId, Neighbourhood)] = {
    in.flatMap { case (a,b) => Seq((a,b),(b,a)) }
      .groupByKey()
      .map { case (node, neighs) => (node, neighs.distinct.toArray) }
  }

}
